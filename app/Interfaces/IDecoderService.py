from abc import ABC, abstractmethod


class IMisbDecoder(ABC):
    """Contract for decoding a KLV binary file into a .txt file of JSON lines."""

    @abstractmethod
    def decode(self, file_name: str) -> str:
        """Decode file_name and return the path of the written output file."""
        raise NotImplementedError

    @abstractmethod
    def to_dict(self, packet) -> dict:
        """Turn one KLV packet into {tag_name: value}."""
        raise NotImplementedError