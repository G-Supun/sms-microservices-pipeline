import sys
import os
import asyncpg
from typing import List, Tuple, Optional

# Add root workspace to path to resolve shared_libs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared_libs.config import settings

class DatabaseService:
    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                database=settings.DB_NAME,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                min_size=settings.MIN_CONN,
                max_size=settings.MAX_CONN,
                command_timeout=60.0
            )

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def bulk_insert_classifications(self, records: List[Tuple[Optional[str], str, bool]]) -> int:
        if not records:
            return 0
        if self._pool is None:
            raise RuntimeError("Database connection pool is not initialized.")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.copy_records_to_table(
                    table_name="sms_classifications",
                    records=records,
                    columns=["mobile", "message", "is_transactional"]
                )
                try:
                    count_str = result.split()[-1]
                    return int(count_str)
                except Exception:
                    return len(records)

db_service = DatabaseService()