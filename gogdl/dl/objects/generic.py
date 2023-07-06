
class BaseDiff:
    def __init__(self):
        self.deleted = []
        self.new = []
        self.changed = []
        self.redist = []
