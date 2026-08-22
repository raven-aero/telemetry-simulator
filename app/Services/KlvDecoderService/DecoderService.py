import json
import os
import klvdata

from app.Constants.Constants import ProgramConstants
from app.Core.config import settings
from app.Interfaces.IDecoderService import IMisbDecoder 


class MisbDecoder(IMisbDecoder):

    def decode(self, file_name: str) -> str:
        # Generate the output file path using system settings and constants
        out_file_name = os.path.splitext(os.path.basename(file_name))[0] + ProgramConstants.ENCODED_FILE_ENDING
        out_path = os.path.join(settings.STORAGE_DECODED_PATH, out_file_name)

        # Open both files simultaneously: read binary from source, write text to destination
        with open(file_name, ProgramConstants.READ_BIN) as f, \
             open(out_path, ProgramConstants.WRITE, encoding=ProgramConstants.BYTE_ENCODEING) as out:
            
            # Pass the file object 'f' directly to StreamParser instead of reading it all into memory
            for packet in klvdata.StreamParser(f):
                packet_dict = self.to_dict(packet)
                
                # Write to the JSONL file only if the packet was successfully parsed and is not empty
                if packet_dict:
                    out.write(json.dumps(packet_dict, ensure_ascii=False) + "\n")

        return out_path

    def to_dict(self, packet) -> dict:
        """
        Safely converts a KLV packet to a dictionary.
        Includes built-in protection against internal bugs in the klvdata library.
        """
        packet_dict = {}
        
        # klvdata stores packet fields in an 'items' attribute, which is an OrderedDict
        items_dict = getattr(packet, "items", None)
        
        # Ensure the attribute exists and behaves like a dictionary
        if isinstance(items_dict, dict):
            for tag, item in items_dict.items():
                
                # 1. Safely extract the field name
                try:
                    name = getattr(item, 'name', str(tag))
                except Exception:
                    name = str(tag)
                    
                # 2. Safely extract the value (where the klvdata library often crashes)
                val_str = ""
                try:
                    if hasattr(item, 'value'):
                        val = item.value
                    else:
                        val = item
                        
                    # Handle raw bytes to prevent JSON serialization crashes
                    if isinstance(val, bytes):
                        val_str = val.hex()
                    else:
                        val_str = str(val)
                        
                except Exception as e:
                    # Fallback string if the library throws an internal error (e.g., isinstance bug)
                    val_str = f"<Unparseable Field: {e}>"
                    
                packet_dict[str(name)] = val_str
                
        return packet_dict