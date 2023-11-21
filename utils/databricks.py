class Widgets:
    def __init__(self, dbutils):
        self.dbutils = dbutils
    
    def create_text(self, tag, default):
        self.dbutils.widgets.text(tag, default, tag)

    def create_dropdown(self, tag, seq: list, default=None):
        if default is None:
            default = seq[0]
        self.dbutils.widgets.dropdown(tag, default, seq, tag)

    def remove_all(self):
        self.dbutils.widgets.removeAll()

    def __call__(self, name):
        return self.dbutils.widgets.get(name)
    
    def get(self, *args):
        return self.__call__(*args)