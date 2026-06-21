"""Daemon scheduler for continuous migrations (Functional).

Manages scheduled syncs and one-time jobs using a dedicated SQLite database.
"""

import logging
import os
import sqlite3
import time
from typing import List, Optional

from migrator.config import AppConfig, FolderJob
from migrator.types import SchedulerContext, S3Context
from migrator.s3_client import S3AuthError, create_s3_context
from migrator.transfer import _process_folder, _resolve_tracking_dir, MigrationStats, FatalMigrationError


logger = logging.getLogger("migrator.scheduler")


def create_scheduler_context(db_path: str) -> SchedulerContext:
    """Initialize SchedulerDB context."""
    conn = sqlite3.connect(db_path)
    ctx = SchedulerContext(db_path=db_path, conn=conn)
    _init_db(ctx)
    return ctx

def _init_db(ctx: SchedulerContext):
    with ctx.conn:
        ctx.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                folder_path TEXT PRIMARY KEY,
                cron_expr TEXT,
                last_run REAL,
                next_run REAL,
                status TEXT
            )
            """
        )

def sync_jobs(ctx: SchedulerContext, jobs: List[FolderJob]):
    """Upsert jobs from folders.txt into the database."""
    now = time.time()
    
    with ctx.conn:
        ctx.conn.row_factory = sqlite3.Row
        existing = {row["folder_path"]: row for row in ctx.conn.execute("SELECT * FROM jobs")}

        for job in jobs:
            path = job.path
            cron = job.cron_expr

            if path not in existing:
                # New job
                next_run = None
                if cron:
                    from croniter import croniter
                    next_run = croniter(cron, now).get_next()
                else:
                    # One-time job runs immediately
                    next_run = now

                ctx.conn.execute(
                    "INSERT INTO jobs (folder_path, cron_expr, next_run) VALUES (?, ?, ?)",
                    (path, cron, next_run),
                )
                logger.info("Added new job for %s (cron: %s)", path, cron or "One-time")
            else:
                # Existing job
                row = existing[path]
                db_cron = row["cron_expr"]
                status = row["status"]

                updates = {}
                
                if cron != db_cron:
                    # Cron changed
                    updates["cron_expr"] = cron
                    if cron:
                        from croniter import croniter
                        updates["next_run"] = croniter(cron, now).get_next()
                    else:
                        # Changed from scheduled to one-time. If it's already completed, don't run.
                        if status != "completed":
                            updates["next_run"] = now

                    logger.info("Updated cron for %s from '%s' to '%s'", path, db_cron, cron)

                # If one-time job failed on previous run, retry it now
                if not cron and status == "failed" and row["next_run"] is None:
                    updates["next_run"] = now
                    logger.info("Retrying failed one-time job for %s", path)

                if updates:
                    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
                    values = list(updates.values()) + [path]
                    ctx.conn.execute(f"UPDATE jobs SET {set_clause} WHERE folder_path = ?", values)


def get_ready_jobs(ctx: SchedulerContext, now: float) -> List[str]:
    with ctx.conn:
        cursor = ctx.conn.execute(
            "SELECT folder_path FROM jobs WHERE next_run IS NOT NULL AND next_run <= ?",
            (now,)
        )
        return [row[0] for row in cursor]
        
def get_job_cron(ctx: SchedulerContext, path: str) -> Optional[str]:
    with ctx.conn:
        cursor = ctx.conn.execute("SELECT cron_expr FROM jobs WHERE folder_path = ?", (path,))
        row = cursor.fetchone()
        return row[0] if row else None

def record_run(ctx: SchedulerContext, path: str, status: str, next_run: Optional[float]):
    now = time.time()
    with ctx.conn:
        ctx.conn.execute(
            "UPDATE jobs SET last_run = ?, next_run = ?, status = ? WHERE folder_path = ?",
            (now, next_run, status, path)
        )

def get_next_sleep_time(ctx: SchedulerContext, now: float) -> float:
    with ctx.conn:
        cursor = ctx.conn.execute("SELECT MIN(next_run) FROM jobs WHERE next_run IS NOT NULL AND next_run > ?", (now,))
        row = cursor.fetchone()
        if row and row[0]:
            return max(1.0, row[0] - now)
        return 60.0 # Default sleep if no upcoming jobs


def run_daemon(config: AppConfig, folders_path: str, dry_run: bool) -> int:
    """Run the migrator as a persistent daemon scheduler."""
    logger.info("Starting Daemon Scheduler")
    
    tracking_dir = _resolve_tracking_dir(config.base_dir)
    db_path = os.path.join(tracking_dir, "scheduler.db")
    sched_ctx = create_scheduler_context(db_path)

    # Boot phase: sync jobs from folders.txt into DB
    sync_jobs(sched_ctx, config.folders)
    
    # Initialize S3 client once for the daemon
    try:
        s3_ctx = create_s3_context(config.s3)
    except S3AuthError as e:
        logger.critical("FATAL: S3 authentication failed: %s", e)
        return 2

    multipart_threshold = config.options.multipart_threshold_mb * 1024 * 1024
    
    try:
        while True:
            now = time.time()
            ready_jobs = get_ready_jobs(sched_ctx, now)
            
            for path in ready_jobs:
                logger.info("=" * 60)
                logger.info("DAEMON: Executing job for %s", path)
                logger.info("=" * 60)
                
                cron_expr = get_job_cron(sched_ctx, path)
                
                stats = MigrationStats(start_time=time.time())
                job_status = "failed"
                
                try:
                    # In continuous/daemon mode, we do mirror sync logic for ALL jobs (even one-time)
                    # Because they might be picking up where they left off.
                    _process_folder(
                        folder=path,
                        config=config,
                        s3_ctx=s3_ctx,
                        tracking_dir=tracking_dir,
                        folders_path=folders_path,
                        multipart_threshold=multipart_threshold,
                        dry_run=dry_run,
                        continuous=True, 
                        stats=stats,
                    )
                    job_status = "completed" if stats.failed == 0 else "failed"
                except FatalMigrationError as e:
                    logger.error("Daemon caught fatal error for %s: %s", path, e)
                    job_status = "failed"
                except Exception as e:
                    logger.error("Daemon caught unexpected error for %s: %s", path, e)
                    job_status = "failed"
                
                logger.info(stats.summary())
                
                # Calculate next run
                next_run = None
                if cron_expr:
                    from croniter import croniter
                    # Use the current time to compute the next valid trigger
                    next_run = croniter(cron_expr, time.time()).get_next()
                    logger.info("Scheduled job %s finished. Next run at epoch %.1f", path, next_run)
                else:
                    if job_status == "completed":
                        logger.info("One-time job %s completed successfully. Will not run again.", path)
                    else:
                        logger.warning("One-time job %s failed. Will retry on next daemon boot.", path)
                
                record_run(sched_ctx, path, job_status, next_run)

            # Sleep until next job
            now = time.time()
            sleep_sec = get_next_sleep_time(sched_ctx, now)
            logger.debug("Daemon sleeping for %.1f seconds...", sleep_sec)
            time.sleep(sleep_sec)

    except KeyboardInterrupt:
        logger.info("Daemon received shutdown signal. Exiting gracefully.")
        return 0
