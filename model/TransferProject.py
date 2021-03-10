class TransferProject:

    @property
    def input_file(self):
        return self._input_file

    @property
    def target_field(self):
        return self._target_field

    @property
    def target_subfield(self):
        return self._target_subfield

    @property
    def target_indicator_1(self):
        return self._target_indicator_1

    @property
    def target_indicator_2(self):
        return self._target_indicator_2

    @property
    def exclude_list(self):
        return self._exclude_list

    @exclude_list.setter
    def exclude_list(self, exclude_list):
        self._exclude_list = exclude_list

    def __init__(self, input_file, target_field, target_subfield, target_indicator_1, target_indicator_2, exclude_list):
        self._input_file = input_file
        self._target_field = target_field
        self._target_subfield = target_subfield
        self._target_indicator_1 = target_indicator_1
        self._target_indicator_2 = target_indicator_2
        self._exclude_list = exclude_list

