import json

class Extractor():
    def write_instance(self, output_stream, instance_dict):
        pass

class NDJsonExtractor(Extractor):
    def write_instance(self, output_stream, instance_dict):
        json_str = json.dumps(instance_dict, indent=None, sort_keys=False, separators=(',', ': '), ensure_ascii=False)
        output_stream.write(json_str + '\n')