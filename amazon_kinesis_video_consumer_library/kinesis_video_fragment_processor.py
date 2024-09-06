"""
Amazon Kinesis Video Stream (KVS) Consumer Library for Python.

This class provides post-processing fiunctions for a MKV fragement that has been parsed
by the Amazon Kinesis Video Streams Cosumer Library for Python.

 """

__version__ = "0.0.2"
__status__ = "Development"
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>, Nicolas Neudeck <https://www.linkedin.com/in/nicolasneudeck/>"

import io
import logging

import amazon_kinesis_video_consumer_library.ebmlite.util as emblite_utils

# Init the logger.
log = logging.getLogger(__name__)


class KvsFragementProcessor:

    ####################################################
    # Fragment processing functions

    def get_fragment_tags(self, fragment_dom):
        """
        Parses a MKV Fragment Doc (of type ebmlite.core.MatroskaDocument) that is returned to the provided callback
        from get_streaming_fragments() in this class and returns a dict of the SimpleTag elements found.

        ### Parameters:

            **fragment_dom**: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                The DOM like structure describing the fragment parsed by EBMLite.

        ### Returns:

            simple_tags: dict

            Dictionary of all SimpleTag elements with format -  TagName<String> : TagValue <String | Binary>.

        """

        # Get the Segment Element of the Fragment DOM - error if not found
        segment_element = None
        for element in fragment_dom:
            if element.id == 0x18538067:  # MKV Segment Element ID
                segment_element = element
                break

        if not segment_element:
            raise KeyError("Segment Element required but not found in fragment_doc")

        # Save all of the SimpleTag elements in the Segment element
        simple_tag_elements = []
        for element in segment_element:
            if element.id == 0x1254C367:  # Tags element type ID
                for tags in element:
                    if tags.id == 0x7373:  # Tag element type ID
                        for tag_type in tags:
                            if tag_type.id == 0x67C8:  # SimpleTag element type ID
                                simple_tag_elements.append(tag_type)

        # For all SimpleTags types (ID: 0x67C8), save for TagName (ID: 0x7373) and values of TagString (ID:0x4487) or TagBinary (ID: 0x4485 )
        simple_tags_dict = {}
        for simple_tag in simple_tag_elements:

            tag_name = None
            tag_value = None
            for element in simple_tag:
                if element.id == 0x45A3:  # Tag Name element type ID
                    tag_name = element.value
                elif (
                    element.id == 0x4487 or element.id == 0x4485
                ):  # TagString and TagBinary element type IDs respectively
                    tag_value = element.value

            # As long as tag name was found add the Tag to the return dict.
            if tag_name:
                simple_tags_dict[tag_name] = tag_value

        return simple_tags_dict

    def get_simple_block_offset(self, fragment_dom):
        # Get the Segment Element of the Fragment DOM - error if not found
        segment_element = None
        for element in fragment_dom:
            if element.id == 0x18538067:  # MKV Segment Element ID
                segment_element = element
                break
        if not segment_element:
            raise KeyError("Segment Element required but not found in fragment_doc")

        # offset = 40
        simple_block_elements = []
        for element in segment_element:
            if element.id == 0x1F43B675:
                for el in element:
                    if el.id == 0xA3:
                        simple_block_elements.append((el.payloadOffset, el.size))
                break
            # else :
            #     offset += int(element.length)
        return simple_block_elements

    def get_fragement_dom_pretty_string(self, fragment_dom):
        """
        Returns the Pretty Print parsing of the EBMLite fragment DOM as a string

        ### Parameters:

            **fragment_dom**: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                The DOM like structure describing the fragment parsed by EBMLite.

        ### Return:
            **pretty_print_str**: str
                Pretty print string of the Fragment DOM object
        """

        pretty_print_str = io.StringIO()

        emblite_utils.pprint(fragment_dom, out=pretty_print_str)
        return pretty_print_str.getvalue()

    def parse_vint(self, data, offset=0):
        """
        Parse an EBML variable size integer (vint) used in Matroska.
        Returns a tuple (value, size) where 'value' is the extracted integer
        and 'size' is the size in bytes of the vint.
        """
        first_byte = data[offset]
        mask = 0x80
        size = 1

        while size <= 8 and not (first_byte & mask):
            mask >>= 1
            size += 1

        if size > 8:
            raise ValueError("Invalid VINT size (>8 bytes)")

        value = first_byte & (mask - 1)
        for i in range(1, size):
            value = (value << 8) | data[offset + i]

        return value, size

    def extract_simpleblock_data(self, byte_array):
        """
        Extracts the track number and the data payload from a Matroska SimpleBlock bytearray.
        """
        # Parse track number
        track_number, size = self.parse_vint(byte_array, 0)

        # Skip the track number and timestamp (2 bytes) to get to the payload
        payload_offset = (
            size + 2 + 1
        )  # 2 bytes for the timestamp and 1 byte for the flags

        # Read the rest of the data as the payload
        data_payload = byte_array[payload_offset:]

        return track_number, data_payload

    def get_audio_tracks(self, fragment_dom):
        audio_from_customer_track, audio_to_customer_track = 0, 0
        # Get the Segment Element of the Fragment DOM - error if not found
        segment_element = None
        for element in fragment_dom:
            if element.id == 0x18538067:  # MKV Segment Element ID
                segment_element = element
                break
        if not segment_element:
            raise KeyError("Segment Element required but not found in fragment_doc")

        for element in segment_element:
            if element.id == 0x1654AE6B:  # Tracks
                for el in element:
                    if el.id == 0xAE:
                        track_number = -1
                        track_name = ""
                        for e in el:
                            if e.id == 0x536E:  # TrackName
                                track_name = e.value
                            elif e.id == 0xD7:  # TrackNumber
                                track_number = e.value
                        if track_name == "AUDIO_FROM_CUSTOMER":
                            audio_from_customer_track = track_number
                        elif track_name == "AUDIO_TO_CUSTOMER":
                            audio_to_customer_track = track_number
        if audio_from_customer_track == 0 or audio_to_customer_track == 0:
            log.error("Audio tracks not found in the fragment")
            audio_from_customer_track = 1
            audio_to_customer_track = 2
        return int(audio_from_customer_track), int(audio_to_customer_track)
