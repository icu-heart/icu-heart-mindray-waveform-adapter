"""High-performance, Mindray HL7 to intermediate waveform signal data extractor."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Tuple, NamedTuple
import hl7
import time


# ---- Custom Exceptions ----
"""Base exception for all fatal HL7 processing errors."""
class HL7ProcessingError(Exception):
    @property
    # i.e Convert "FormatError" -> "hl7_format_error"
    def error_queue(self) -> str:
        name = self.__class__.__name__
        result = []
        for char in name:
            if char.isupper():
                result.append('_')
            result.append(char.lower())
        return f"hl7{''.join(result)}"

"""Raised when the message is fundamentally unparseable.""" 
class FormatError(HL7ProcessingError): pass

"""Raised when a required field is missing from the message."""
class MissingFieldError(HL7ProcessingError): pass

"""Raised when a timestamp is malformed or out of range."""
class InvalidTimestampError(HL7ProcessingError): pass

"""Raised when a part of the message is structurally incorrect.""" 
class SignalDataError(HL7ProcessingError): pass   

# ---- Data Structures for Extracted Data ----

class SignalData(NamedTuple):
    """A simple container for a single signal's data and signal specific metadata."""
    name: str
    values: str
    units: str
    frequency: int
    resolution: float

@dataclass
class SignalDataBundle:
    """A simple container for overall metadata and multiple signal data extracted from the HL7 message."""
    ward: str
    bed: str
    timestamp_ns: int
    signals: List[SignalData] = field(default_factory=list)


# ---- The Extractor ----

class SignalDataExtractor:
    """Simple extractor that translates HL7 bytestring into SignalDataBundle."""

    def _parse(self, bytestring: bytes):
        try:
            return hl7.parse(bytestring.decode('utf-8', errors='ignore').replace("\n", "\r"))
        except (UnicodeDecodeError, hl7.exceptions.ParseException) as e:
            raise FormatError(f"Failed to parse HL7 message: {e}") from e

    def _extract_bedspace(self, message) -> Tuple[str, str]:
        try:
            pv1 = message["PV1"][0][3][0]
            return pv1[0][0], pv1[-1][0] # Ward, Bedspace
        except IndexError as e:
            raise MissingFieldError("Could not extract bedspace from PV1 segment.") from e

    def _extract_timestring(self, message) -> str:
        try:
            return message["OBR"][0][-2][0].split(".")[0]
        except IndexError as e:
            raise MissingFieldError("Could not extract timestring from OBR segment.") from e

    def _collect_signals(self, message) -> List[SignalData]:
        signals: List[SignalData] = []
        segs = list(message["OBX"])
        i = 0
        while i < len(segs):
            obx = segs[i]
            if obx[2][0] != "NA":
                i += 1
                continue
            try:
                name = obx[3][0][1][0]
                values = str(obx[5][0])
                freq = int(segs[i + 1][5][0])
                res = float(segs[i + 2][5][0])
                units = str(segs[i + 2][6][0][1][0])
            except (IndexError, ValueError, TypeError) as e:
                raise SignalDataError(f"Malformed OBX group near signal '{obx[3][0][1][0]}': {e}") from e

            signals.append(SignalData(name=name, values=values, units=units, frequency=freq, resolution=res))
            i += 3
        return signals

    @staticmethod
    def _timestring_to_ns(ts: str) -> int:
        try:
            # Fast path for YYYYMMDDHHMMSS format
            year = int(ts[0:4]); month = int(ts[4:6]); day = int(ts[6:8])
            hour = int(ts[8:10]); minute = int(ts[10:12]); second = int(ts[12:14])
            return int(time.mktime((year, month, day, hour, minute, second, 0, 0, -1)) * 1e9)
        except (ValueError, OverflowError) as e:
            raise InvalidTimestampError(f"Failed to convert timestamp '{ts}': {e}") from e

    def extract_data(self, bytestring: bytes) -> SignalDataBundle:
        message = self._parse(bytestring)
        ward, bed = self._extract_bedspace(message)
        timestamp_ns = self._timestring_to_ns(self._extract_timestring(message))
        signals = self._collect_signals(message)
        return SignalDataBundle(ward=ward, bed=bed, timestamp_ns=timestamp_ns, signals=signals)