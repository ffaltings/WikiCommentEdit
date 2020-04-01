"""
Idea: Filters as first-class objects can be stacked at runtime.
"""

class WikiFilter():
    """WikiFilters are callables supporting any of apply_meta, apply_instance, apply_pre_diff, apply_post_diff"""

    def apply_meta(self, meta):
        """Applied when meta data is parsed"""
        return True

    def apply_pre_diff(self, instance):
        """Applied before tokenization and before diff is computed"""
        return True

    def apply_post_diff(self, instance):
        """Applied after tokenization and after diff is computed"""
        return True

    def apply_instance(self, instance):
        """Applied to the final instance"""
        return True

class CommentLength(WikiFilter):
    def __init__(self, min_len, max_len):
        self.min_len = min_len
        self.max_len = max_len

    def apply_meta(self, meta):
        clen = len(meta["comment_text"])
        return clen >= self.min_len and clen < self.max_len


class TextLength(WikiFilter):
    def __init__(self, min_len, max_len):
        self.min_len = min_len
        self.max_len = max_len

    def apply_pre_diff(self, instance):
        src_text = instance["src_text"]
        tgt_text = instance["tgt_text"]
        len_src = len(src_text)
        len_tgt = len(tgt_text)
        return src_text and tgt_text and len_src >= self.min_len and len_tgt >= self.min_len and len_src < self.max_len and len_tgt < self.max_len


class HasSectionTitle(WikiFilter):
    def apply_meta(self, meta):
        return meta["section_title"] is not None

class IsHumanEdit(WikiFilter):
    def apply_instance(self, instance):
        return True

class HasGrounding(WikiFilter):
    def apply_instance(self, instance):
        if "http://" in instance["src_text"]:
            return True
        else:
            return False

class ExtractLeftRightContext(WikiFilter):
    def __init__(self, left_window_size, right_window_size):
        self.left_window_size = left_window_size
        self.right_window_size = right_window_size

    def apply_post_diff(self, instance):
        if len(instance["src_action"]) < 1:
            return False
        start_token_idx = instance["src_action"][0]
        end_token_idx = instance["src_action"][-1]

        return True

