"""
Database module for metadata storage using SQLite
Stores job information, tile metadata, and worker status
"""

import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional
import config

class MetadataDB:
    def __init__(self, db_path: str = config.DATABASE_PATH):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Initialize database tables"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Jobs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                effect TEXT NOT NULL,
                total_tiles INTEGER NOT NULL,
                completed_tiles INTEGER DEFAULT 0,
                original_width INTEGER NOT NULL,
                original_height INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                processing_time REAL
            )
        ''')
        
        # Tiles table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tiles (
                tile_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                tile_index INTEGER NOT NULL,
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                width INTEGER NOT NULL,
                height INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                worker_id TEXT,
                processing_time REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            )
        ''')
        
        # Workers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'active',
                last_heartbeat TIMESTAMP,
                total_tasks_processed INTEGER DEFAULT 0,
                avg_processing_time REAL DEFAULT 0.0,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ Database initialized")
    
    # ========== Job Operations ==========
    
    def create_job(self, job_id: str, effect: str, total_tiles: int, 
                   width: int, height: int) -> bool:
        """Create a new job entry"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO jobs (job_id, status, effect, total_tiles, 
                                original_width, original_height, started_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (job_id, 'processing', effect, total_tiles, width, height))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error creating job: {e}")
            return False
    
    def update_job_progress(self, job_id: str, completed_tiles: int) -> bool:
        """Update job progress"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE jobs 
                SET completed_tiles = ?
                WHERE job_id = ?
            ''', (completed_tiles, job_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating job progress: {e}")
            return False
    
    def complete_job(self, job_id: str, processing_time: float) -> bool:
        """Mark job as completed"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE jobs 
                SET status = 'completed', 
                    completed_at = CURRENT_TIMESTAMP,
                    processing_time = ?
                WHERE job_id = ?
            ''', (processing_time, job_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error completing job: {e}")
            return False
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job details"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return dict(row)
            return None
        except Exception as e:
            print(f"Error getting job: {e}")
            return None
    
    def get_all_jobs(self, limit: int = 50) -> List[Dict]:
        """Get all jobs (recent first)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM jobs 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (limit,))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error getting jobs: {e}")
            return []
    
    # ========== Tile Operations ==========
    
    def create_tile(self, tile_id: str, job_id: str, tile_index: int,
                   x: int, y: int, width: int, height: int) -> bool:
        """Create a new tile entry"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO tiles (tile_id, job_id, tile_index, x, y, width, height)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (tile_id, job_id, tile_index, x, y, width, height))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error creating tile: {e}")
            return False
    
    def update_tile_status(self, tile_id: str, status: str, 
                          worker_id: str = None, 
                          processing_time: float = None) -> bool:
        """Update tile processing status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if status == 'completed':
                cursor.execute('''
                    UPDATE tiles 
                    SET status = ?, worker_id = ?, processing_time = ?,
                        processed_at = CURRENT_TIMESTAMP
                    WHERE tile_id = ?
                ''', (status, worker_id, processing_time, tile_id))
            else:
                cursor.execute('''
                    UPDATE tiles 
                    SET status = ?
                    WHERE tile_id = ?
                ''', (status, tile_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating tile status: {e}")
            return False
    
    # ========== Worker Operations ==========
    
    def update_worker_heartbeat(self, worker_id: str) -> bool:
        """Update worker heartbeat"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO workers (worker_id, last_heartbeat)
                VALUES (?, CURRENT_TIMESTAMP)
                ON CONFLICT(worker_id) DO UPDATE SET
                    last_heartbeat = CURRENT_TIMESTAMP,
                    status = 'active'
            ''', (worker_id,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating worker heartbeat: {e}")
            return False
    
    def update_worker_stats(self, worker_id: str, processing_time: float) -> bool:
        """Update worker processing statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE workers
                SET total_tasks_processed = total_tasks_processed + 1,
                    avg_processing_time = (
                        (avg_processing_time * total_tasks_processed + ?) / 
                        (total_tasks_processed + 1)
                    )
                WHERE worker_id = ?
            ''', (processing_time, worker_id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating worker stats: {e}")
            return False
    
    def get_active_workers(self, timeout_seconds: int = 15) -> List[Dict]:
        """Get list of active workers"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM workers
                WHERE status = 'active'
                AND datetime(last_heartbeat) > datetime('now', '-' || ? || ' seconds')
                ORDER BY last_heartbeat DESC
            ''', (timeout_seconds,))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error getting active workers: {e}")
            return []
    
    def mark_worker_inactive(self, worker_id: str) -> bool:
        """Mark worker as inactive"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE workers 
                SET status = 'inactive'
                WHERE worker_id = ?
            ''', (worker_id,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error marking worker inactive: {e}")
            return False
    
    # ========== Statistics ==========
    
    def get_statistics(self) -> Dict:
        """Get overall system statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) as total FROM jobs')
            total_jobs = cursor.fetchone()['total']
            
            cursor.execute("SELECT COUNT(*) as total FROM jobs WHERE status = 'completed'")
            completed_jobs = cursor.fetchone()['total']
            
            cursor.execute("SELECT COUNT(*) as total FROM jobs WHERE status = 'processing'")
            active_jobs = cursor.fetchone()['total']
            
            cursor.execute('''
                SELECT AVG(processing_time) as avg_time 
                FROM jobs 
                WHERE status = 'completed'
            ''')
            avg_time_row = cursor.fetchone()
            avg_processing_time = avg_time_row['avg_time'] if avg_time_row['avg_time'] else 0
            
            cursor.execute("SELECT COUNT(*) as total FROM tiles WHERE status = 'completed'")
            total_tiles = cursor.fetchone()['total']
            
            conn.close()
            
            return {
                'total_jobs': total_jobs,
                'completed_jobs': completed_jobs,
                'active_jobs': active_jobs,
                'avg_processing_time': round(avg_processing_time, 2),
                'total_tiles_processed': total_tiles
            }
        except Exception as e:
            print(f"Error getting statistics: {e}")
            return {}
