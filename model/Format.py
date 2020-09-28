class Format(object):

    @property
    def end_id(self):
        return self._end_id

    @end_id.setter
    def end_id(self, end_id):
        self._end_id = end_id

    @property
    def field_start(self):
        return self._field_start

    @field_start.setter
    def field_start(self, field_start):
        self._field_start = field_start

    @property
    def field_end(self):
        return self._field_end

    @field_end.setter
    def field_end(self, field_end):
        self._field_end = field_end

    @property
    def indicator(self):
        return self._indicator

    @indicator.setter
    def indicator(self, indicator):
        self._indicator = indicator

    @property
    def value_start(self):
        return self._value_start

    @value_start.setter
    def value_start(self, value_start):
        self._value_start = value_start

    # initializes Format
    def __init__(self, format=''):
        if format == 'marc':
            self._end_id = 9
            self._field_start = 10
            self._field_end = 15
            self._indicator = 16
            self._value_start = 18
        else:
            self._end_id = 9
            self._field_start = 11
            self._field_end = 15
            self._indicator = 16
            self._value_start = 17