# app/dependencies.py
from fastapi import Depends
from app.Interfaces.Itelemetry_files_service import ITelemetryFilesService
from app.Services.TelemetrySimulatorServices.KlvFilesService.Telemetry_file_service import TelemetryFilesService
from app.Interfaces.IDecoderService import IMisbDecoder
from app.Services.KlvDecoderService.DecoderService import MisbDecoder

#-----------singaltons---------------------------
_misb_decoder_instance: IMisbDecoder = MisbDecoder()

def get_misb_decoder() -> IMisbDecoder:
    return _misb_decoder_instance

def get_telemetry_service(decoder: IMisbDecoder = Depends(get_misb_decoder)) -> ITelemetryFilesService:
    return TelemetryFilesService(decoder)