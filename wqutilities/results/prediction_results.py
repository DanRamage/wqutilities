class data_result_types:
  NO_TYPE = 0
  SAMPLING_DATA_TYPE = 1
  MODEL_DATA_TYPE = 2


class wq_defines:
  NO_DATA = -9999.0

class prediction_levels:
  NO_TEST = -1
  LOW = 1
  MEDIUM = 2
  HIGH = 3
  def __init__(self, value):
    self.value = value
  def __str__(self):
    if(self.value == self.LOW):
      return "LOW"
    elif(self.value == self.MEDIUM):
      return "MEDIUM"
    elif(self.value == self.HIGH):
      return "HIGH"
    else:
      return "NO TEST"