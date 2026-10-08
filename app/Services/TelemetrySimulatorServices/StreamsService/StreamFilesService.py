import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Dict, Set

import aiofiles
from app.Constants.Constants import KafkaConst
from app.Constants.StreamMessages import StreamMessages
from app.Core.config import settings
from app.DTOs.DBDTOs import AddChannelDbDTO, FileType
from app.DTOs.KafkaDTOs import KafkaMessageDTO
from app.DTOs.StreamsDTOs import StartStreamDTO, StopStreamDTO
from app.Interfaces.IDBManager import IDBManager
from app.Interfaces.IKafkaProducerService import IKafkaProducerService
from app.Interfaces.IStreamFilesService import IStreamFilesService
from app.ROSs.StreamsROs import (
    StartStreamErrorResponse,
    StartStreamSuccessResponse,
    StopStreamErrorResponse,
    StopStreamSuccessResponse,
)

logger = logging.getLogger(__name__)


class StreamFilesService(IStreamFilesService):

    def __init__(self, kafka_producer: IKafkaProducerService, db_manager: IDBManager):
        self._producer = kafka_producer
        self._db_manager = db_manager
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._file_to_partition: Dict[str, int] = {}
        self._storage_path = Path(settings.STORAGE_DECODED_PATH)

    def _get_partition(self, file_name: str) -> int:
        """Find the lowest available partition index (0-9)."""
        if file_name in self._file_to_partition:
            return self._file_to_partition[file_name]
        used_partitions: Set[int] = set(self._file_to_partition.values())
        for partition_id in range(KafkaConst.MAX_PARTITIONS):
            if partition_id not in used_partitions:
                self._file_to_partition[file_name] = partition_id
                return partition_id
        raise RuntimeError(StreamMessages.ALL_PARTITIONS_USED)

    async def _stream_file_worker(self, file_name: str, file_path: Path, partition: int) -> None:
        """Read file line by line and route directly to the designated partition."""
        topic = settings.MAIN_TOPIC_NAME
        try:
            async with aiofiles.open(file_path, mode="r", encoding="utf-8") as f:
                while True:
                    previous_time: datetime | None = None   
                    async for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        frame_data = json.loads(line)
                        drone_id = frame_data.get(StreamMessages.DRONE_ID_KEY, file_name)

                        # getting the PTS from the packet
                        pts_raw = frame_data.get(StreamMessages.PTS_KEY)
                        pts = datetime.fromisoformat(pts_raw) if pts_raw else None

                        # calc the time to await
                        if previous_time is not None and pts is not None:
                            await_time = (pts - previous_time).total_seconds()
                            
                            if 0 < await_time < 1.0:
                                await asyncio.sleep(await_time)
                            elif await_time < 0 or await_time >= 1.0:
                                logger.warning(StreamMessages.NOT_VALID_PTS_DIFF.format(await_time))
                        
                        if pts is not None:
                            previous_time = pts
                        # chaning the UTC to current utc
                        frame_data[StreamMessages.CURRENT_PTS_KEY] = datetime.now(timezone.utc).isoformat()
                        message = KafkaMessageDTO(
                            topic=topic,
                            value=frame_data,
                            key=drone_id,
                            partition=partition,
                        )
                        await self._producer.send_message(message)

                    logger.info(StreamMessages.SENDING_IN_LOOP.format(file_name))
                    await f.seek(0)
                    previous_time = None

        except asyncio.CancelledError:
            logger.info(StreamMessages.STREAM_STOPPED.format(file_name))
            raise
        except Exception as e:
            logger.error(StreamMessages.ERROR_STREAMING, file_name, e)
        finally:
            self._active_tasks.pop(file_name, None)

    async def start_stream_file(
        self, request: StartStreamDTO
    ) -> StartStreamSuccessResponse | StartStreamErrorResponse:
        sim_id = request.sim_id

        file_name = await self._db_manager.get_source_file_path_by_id(sim_id)
        if not file_name:
            return StartStreamErrorResponse(
                message=StreamMessages.STREAM_NOT_FOUND_IN_DB.format(StartStreamDTO.sim_id)
            )

        if file_name in self._active_tasks:
            return StartStreamErrorResponse(
                message=StreamMessages.STREAM_ALREADY_RUNNING.format(file_name)
            )

        file_path = self._storage_path / file_name
        if not file_path.exists():
            return StartStreamErrorResponse(
                message=StreamMessages.FILE_NOT_FOUND.format(file_name)
            )

        try:
            partition = self._get_partition(file_name)

            add_channel_dto = AddChannelDbDTO(
                source_file_id=sim_id,
                kafka_partition=partition,
                file_type=FileType.DECODED,
            )
            await self._db_manager.add_channel(add_channel_dto)

            task = asyncio.create_task(
                self._stream_file_worker(file_name, file_path, partition)
            )
            self._active_tasks[file_name] = task

            return StartStreamSuccessResponse(
                message=StreamMessages.STREAM_SUCCES.format(file_name, partition)
            )
        except Exception as e:
            self._file_to_partition.pop(file_name, None)
            logger.error(StreamMessages.FAILD_TO_START.format(file_name, e))
            return StartStreamErrorResponse(
                message=StreamMessages.INTERNAL_ERROR.format(str(e))
            )

    async def stop_stream_file(
        self, request: StopStreamDTO
    ) -> StopStreamSuccessResponse | StopStreamErrorResponse:
        sim_id = request.sim_id

        file_name = await self._db_manager.get_source_file_path_by_id(sim_id)
        if not file_name:
            return StopStreamErrorResponse(
                message=StreamMessages.STREAM_NOT_FOUND_IN_DB.format(StopStreamDTO.sim_id)
            )

        task = self._active_tasks.get(file_name)

        if not task or task.done():
            return StopStreamErrorResponse(
                message=StreamMessages.STREAM_NOT_FOUND.format(file_name)
            )

        try:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return StopStreamSuccessResponse(
                message=StreamMessages.STREAM_STOPPED.format(file_name)
            )
        except Exception as e:
            logger.error(StreamMessages.FAILD_TO_STOP_STREAM.format(file_name, e))
            return StopStreamErrorResponse(
                message=StreamMessages.INTERNAL_ERROR.format(str(e))
            )