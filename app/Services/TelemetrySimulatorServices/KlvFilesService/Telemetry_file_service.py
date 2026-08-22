# app/services/telemetry_files_service.py
import os
import aiofiles
import aiofiles.os 
from fastapi import UploadFile
from app.Interfaces.Itelemetry_files_service import ITelemetryFilesService
from app.Core.config import settings
from app.ROSs.ReciveFileRos import *
from app.Constants.ReciveFileMessages import *  
from app.Constants.Constants import ProgramConstants
from app.Interfaces.IDecoderService import IMisbDecoder
import asyncio

class TelemetryFilesService(ITelemetryFilesService):
    def __init__(self, decoder: IMisbDecoder):
        self.decoder = decoder

    #----------files - Recive and save file----------------------------------------------------
    async def Recive_file(self, file: UploadFile) -> FileSuccessResponse | FileErrorResponse:
        if not self.is_extentsion_valid(file.filename):
            msg = FilesControllerROsMessages.Error.EXTENTSION_NOT_VALID
            return FileErrorResponse(message=FilesControllerROsMessages.Error.FILE_SAVE_FAILED_TEMPLATE.format(file.filename, msg))
        
        uploadFile = settings.STORAGE_PATH

        os.makedirs(uploadFile, exist_ok=True)
        file_path = os.path.join(uploadFile, file.filename)

        try:
            async with aiofiles.open(file_path, ProgramConstants.WRITE_BIN) as dst_file:
                while content := await file.read(ProgramConstants.READ_CHUNK_SIZE): 
                    await dst_file.write(content)
                await asyncio.to_thread(self.decoder.decode, file_path)
            return FileSuccessResponse(message=FilesControllerROsMessages.Success.FILE_RECEIVE_AND_SAVE.format(file.filename))
        
        except Exception as e:
            return FileErrorResponse(message=FilesControllerROsMessages.Error.FILE_SAVE_FAILED_TEMPLATE.format(file.filename, e))

        finally:
            await file.close()
            
    #---------------------------end---------------------------------------------------
     
    #interface----------------delete file from the service------------------------------------------- 
    async def Delete_file(self, file_name: str) -> FileSuccessResponse | FileErrorResponse:
        if not self.is_extentsion_valid(file_name):
            msg = FilesControllerROsMessages.Error.EXTENTSION_NOT_VALID
            return FileErrorResponse(message=FilesControllerROsMessages.Error.FILE_SAVE_FAILED_TEMPLATE.format(file_name, msg))
        
        upload_dir = settings.STORAGE_PATH
        file_path = os.path.join(upload_dir, file_name)
        try:
            if not os.path.exists(file_path):
                return FileErrorResponse(
                    message=FilesControllerROsMessages.Error.FILE_DELETE_FAILED_TEMPLATE.format(
                        file_name, ProgramConstants.FILE_NOT_EXISTS
                    )
                )
            
            await aiofiles.os.remove(file_path)

            return FileSuccessResponse(
                message=FilesControllerROsMessages.Success.DELETE_SUCCESS_TEMPLATE.format(file_name)
            ) 
        except Exception as e:
            return FileErrorResponse(
                message=FilesControllerROsMessages.Error.FILE_DELETE_FAILED_TEMPLATE.format(file_name, e)
            )

    #----------------end--------------------------------------------------------------------

    def is_extentsion_valid(self, input: str) -> bool:
        return os.path.splitext(input)[1].lower() == ProgramConstants.VALID_EXTENSION