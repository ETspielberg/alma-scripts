import logging

from model.Format import Format


class LineChecker(object):

    def get_field(self, line):
        return line[self._format.field_start:self._format.field_end]

    def get_value(self, line):
        return line[self._format.value_start:].strip()

    def get_id(self, line):
        return line[0:self._format.end_id]

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
    def method_name(self):
        return self._method_name

    @method_name.setter
    def method_name(self, method_name):
        self._method_name = method_name

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, mode):
        self._mode = mode

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, format):
        self._format = format

    # initializes Line Checker
    def __init__(self, method_name, checklist=None, field='001', position=1, mode="append", format=''):
        """
        initializer for the LineChecker
        :param method_name: the method to be used for checking the line
        :param checklist: a list of Strings to be used for checking the line. If the line is to be chacked against a
        single string, a list of length 1 needs to be provided
        :param field: the field to be analyzed. Default is 001
        """
        self._method_name = method_name
        self._method = getattr(self, method_name, lambda: 0)
        self._field = field
        self._position = position
        self._mode = mode
        if checklist is None:
            self._checklist = [""]
        else:
            self._checklist = checklist
        self._format = Format(format)

    def check(self, line):
        return self._method(line=line)

    def is_on_list(self, line):
        """
        checks whether the field contents is in a provided list of strings
        :param line: the line to be checked
        :return: True, if the field contents is a member of the the predefined list
        """
        if self.get_field(line) == self._field:
            return self.get_field(line) in self._checklist
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
                if check in self.get_value(line):
                    is_contained = True
        return is_contained

    def short_contains(self, line):
        """
        checks whether the provided string contains the short version (3 letters) of a field
        :param line: the line to be checked
        :return: True, if the provided string is found within the field content
        """
        is_contained = False
        if self.get_field(line)[:3] == self._field:
            for check in self._checklist:
                if check in self.get_value(line):
                    is_contained = True
        return is_contained

    def equals(self, line):
        """
        checks whether the provided string is equal to the field contents
        :param line: the line to be checked
        :return: True if the provided string is equal to the field content
        """
        if self.get_field(line) == self._field:
            return self._checklist[0] == self.get_field(line)
        return False

    def starts_with(self, line):
        """
        checks whether the provided value of the provided line starts with a given letter
        :param line: the line to be checked
        :return: True if the provided string starts with the given letter
        """
        if self.get_field(line) == self._field:
            test_string = self._checklist[0]
            number_of_chars = len(test_string)
            if number_of_chars == 1:
                return self._checklist[0] == line[self._format.value_start]
            else:
                return self._checklist[0] == line[self._format.value_start:(self._format.value_start + number_of_chars)]
        return False

    def char_at_position(self, line):
        """
        checks, whether the value at a given position of comprises one of the characters in the checklist
        :param line: the line to be checked
        :return: True if the provided string is equal to the field content
        """
        is_contained = False
        if self.get_field(line) == self._field:
            for check in self._checklist:
                if check[0] == line[self._format.value_start + self._position]:
                    is_contained = True
        return is_contained

    def is_field(self, line):
        """
        checks, whether the line has the correct field
        :param line: the line to be checked
        :return: True if the provided string is equal to the field content
        """
        return self.get_field(line) == self._field

    def is_short_field(self, line):
        """
        checks, whether the line has the correct short field (3 letters)
        :param line: the line to be checked
        :return: True if the provided string is equal to the field content
        """
        return self.get_field(line)[:3] == self._field

    def has_title_sys_id(self, line):
        """
        checks whether a certain identifier is within the identifiers provided as checklist
        :param line: the line to be checked
        :return: True if the provided string has an id at the beginning
        """
        return line[0:self._format.end_id] in self._checklist

    def test_field_values(self, line):
        """
        tests whether the format is correct
        :param line: the line to be checked
        """
        logging.info('id = {}'.format(self.get_id(line)))
        logging.info('field = {}'.format(self.get_field(line)))
        logging.info('value = {}'.format(self.get_value(line)))
