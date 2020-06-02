class LineChecker(object):

    @staticmethod
    def get_field(line):
        return line[11:15]

    # Properties and setters to set the general values such as list to be checked against
    @property
    def checklist(self):
        return self._checklist

    @checklist.setter
    def checklist(self, checklist):
        self._checklist = checklist

    @property
    def field(self):
        return self._field

    @field.setter
    def field(self, field):
        self._field = field

    @property
    def position(self):
        return self._position

    @position.setter
    def position(self, position):
        self._position = position

    @property
    def method(self):
        return self._method_name

    @method.setter
    def method(self, method_name):
        self._method_name = method_name

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def method(self, mode):
        self._mode = mode

    # initializes Line Checker
    def __init__(self, method, checklist=None, field='001', position=1, mode="append"):
        """
        initializer for the LineChecker
        :param method: the method to be used for checking the line
        :param checklist: a list of Strings to be used for checking the line. If the line is to be chacked against a
        single string, a list of length 1 needs to be provided
        :param field: the field to be analyzed. Default is 001
        """
        self._method_name = method
        self._method = getattr(self, method, lambda: 0)
        self._field = field
        self._position = position
        self._mode = mode
        if checklist is None:
            self._checklist = [""]
        else:
            self._checklist = checklist

    def check(self, line):
        return self._method(line=line)

    def is_on_list(self, line):
        """
        checks whether the field contents is in a provided list of strings
        :param line: the line to be checked
        :return: True, if the field contents is a member of the the predefined list
        """
        if self.get_field(line) == self._field:
            return line[18:].strip() in self._checklist
        return False

    def part_on_checklist(self, line):
        """
        checks whether one member of a provided list is part of the field content
        :param line: the line to be checked
        :return: True, if one member of the provided list is part of the field content
        """
        if self.get_field(line) == self._field:
            for entry in self._checklist:
                if entry in line:
                    return True
        return False

    def contains(self, line):
        """
        checks whether the provided string is contained in the field contents
        :param line: the line to be checked
        :return: True, if the provided string is found within the field content
        """
        is_contained = False
        if self.get_field(line) == self._field:
            for check in self._checklist:
                if check in line[18:]:
                    is_contained = True
        return is_contained

    def short_contains(self, line):
        is_contained = False
        if self.get_field(line)[:3] == self._field:
            for check in self._checklist:
                if check in line[18:]:
                    is_contained = True
        return is_contained

    def equals(self, line):
        """
        checks whether the provided string is equal to the field contents
        :param line: the line to be checked
        :return: True if the provided string is equal to the field content
        """
        if self.get_field(line) == self._field:
            return self._checklist[0] == line[18:]
        return False

    def starts_with(self, line):
        if self.get_field(line) == self._field:
            test_string = self._checklist[0]
            number_of_chars = len(test_string)
            if number_of_chars == 1:
                return self._checklist[0] == line[18]
            else:
                return self._checklist[0] == line[18:(18 + number_of_chars)]
        return False

    def char_at_position(self, line):
        is_contained = False
        if self.get_field(line) == self._field:
            for check in self._checklist:
                if check[0] == line[18 + self._position]:
                    is_contained = True
        return is_contained

    def is_field(self, line):
        return self.get_field(line) == self._field

    def is_short_field(self, line):
        return self.get_field(line)[:3] == self._field

    def has_title_sys_id(self, line):
        return line[0:9] in self._checklist
