class ProgramConstants:
    WRITE_BIN = "wb"
    READ_BIN = 'rb'
    WRITE = 'w'
    READ_CHUNK_SIZE = 1024 * 1024
    FILE_NOT_EXISTS = "file is not exists in the services"
    BYTE_ENCODEING = "utf-8"
    ENCODED_FILE_ENDING = '_decoded.txt'
    VALID_EXTENSION = '.bin'

class FastConf:
    #---------fastapi program------------
    TITLE = "Telemetry Simulator API""API for receiving and managing telemetry files"
    DESCRIPTION = "Telemetry Simulator API""API for receiving and managing telemetry files"
    VALID_EXTENSION = ".bin"