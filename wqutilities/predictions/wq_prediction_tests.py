
import logging.config

logger = logging.getLogger(__name__)
class predictionLevels(object):
  DISABLED = -2
  NO_TEST = -1
  LOW = 1
  MEDIUM = 2
  HIGH = 3
  def __init__(self, value):
    self.__value = value
  def __str__(self):
    if self.value >= self.LOW and self.value < self.HIGH:
      return "LOW"
#    elif self.value == self.MEDIUM:
#      return "MEDIUM"
    elif self.value == self.HIGH:
      return "HIGH"
    elif self.value == self.DISABLED:
      return "TEST DISABLED"
    else:
      return "NO TEST"

  @property
  def value(self):
    return self.__value

  @value.setter
  def value(self, value):
    self.__value = value
"""
Class: predictionTest
Purpose: This is the base class for our various prediction tests.
"""
class predictionTest(object):
  """
  Function: __init__
  Purpose: Initialize the object.
  Parameters:
    formula - a string with the appropriate string substitution parameters that the runTest function will
      apply the data against.
    name - A string identifier for the test.
  Return:
  """
  def __init__(self, formula, model_name=None, site_name=None, enabled=True):
    self.formula = formula
    self._predictionLevel = predictionLevels(predictionLevels.NO_TEST)
    self._site_name = site_name
    self._test_time = None
    self._enabled = enabled
    self._test_type = ""
    self._data_used = {}
    self._result = None
    self._model_name = model_name

  @property
  def name(self):
    return(self._model_name)
  @name.setter
  def name(self, name):
    self._model_name = name
  @property
  def model_name(self):
    return(self._model_name)
  @model_name.setter
  def model_name(self, model_name):
    self._model_name = model_name

  @property
  def site_name(self):
    return(self._site_name)
  @site_name.setter
  def model_name(self, site_name):
    self._site_name = site_name


  @property
  def test_time(self):
    return(self._test_time)
  @test_time.setter
  def test_time(self, test_time):
    self._test_time = test_time

  @property
  def enabled(self):
    return(self._enabled)
  @enabled.setter
  def enabled(self, enabled):
    self._enabled = enabled

  @property
  def test_type(self):
    return(self._test_type)
  @test_type.setter
  def test_type(self, test_type):
    self._test_type = test_type

  @property
  def predictionLevel(self):
    return(self._predictionLevel)
  @predictionLevel.setter
  def predictionLevel(self, predictionLevel):
    self._predictionLevel = predictionLevel

  @property
  def result(self):
    return(self._result)
  @result.setter
  def result(self, result):
    self._result = result

  @property
  def data_used(self):
    return(self._data_used)

  """
  Function: runTest
  Purpose: Uses the data parameter to do the string substitutions then evaluate the formula.
  Parameters:
    data - a dictionary with the appropriate keys to do the string subs.
  Return:
    The result of evaluating the formula.
  """
  def runTest(self, data):
    return predictionLevels.NO_TEST

  """
  Function: getResults
  Purpose: Returns a dictionary with the computational variables that went into the predictionLevel. For instance, for an
    MLR calculation, there are intermediate results such as the log10 result and the final result.
  Parameters:
    None
  Return: A dictionary.
  """
  def getResults(self):
    results = {'predictionLevel' : self.predictionLevel.__str__()}
    return results



"""
Class wqTest
Purpose: This is the base class for the actually water quality prediction process.
 Each watershed area has its own MLR and CART tests, so this base class doesn't implement
 anything other than stub functions for them.
"""
class wqEquations(object):
  """
  Function: __init__
  Purpose: Initializes the object with all the tests to be performed for the station.
  Parameters:
    station - The name of the station this object is being setup for.
    model_equation_list - List of model test objects for the site.
    logger - A reference to the logging object to use.
  """
  def __init__(self, station, model_equation_list, use_logger=True):
    self._station = station  #The station that this object represents.
    self._tests = []
    self._ensemblePrediction = predictionLevels(predictionLevels.NO_TEST)
    for model_equation in model_equation_list:
      self._tests.append(model_equation)
    self.data = {} #Data used for the tests.

    self.logger = logger

  @property
  def station(self):
    return(self._station)

  @property
  def tests(self):
    return(self._tests)
  @property
  def ensemblePrediction(self):
    return(self._ensemblePrediction)
  """
  Function: addTest
  Purpose: Adds a prediction test to the list of tests.
  Parameters:
    predictionTestObj -  A predictionTest object to use for testing.
  """
  def addTest(self, predictionTestObj):
    self._tests.append(predictionTestObj)

  """
  Function: runTests
  Purpose: Runs the suite of tests, current a regression formula and CART model, then tabulates
    the overall prediction.
  Parameters:
    dataDict - A data dictionary keyed on the variable names in the CART tree. String subsitution
      is done then the formula is evaled.
  Return:
    A predictionLevels value representing the overall prediction level. This is the average of the individual
    prediction levels.
  """
  def runTests(self, test_data):
    self.data = test_data.copy()

    for testObj in self._tests:
      testObj.runTest(test_data)

    self.overallPrediction()
  """
  Function: overallPrediction
  Purpose: From the models used, averages their predicition values to come up with the overall value.
  Parameters:
    None
  Return:
    A predictionLevels value.
  """
  def overallPrediction(self):
    allTestsComplete = True
    executedTstCnt = 0
    if len(self._tests):
      sum = 0
      for testObj in self._tests:
        #DWR 2011-10-11
        #If a test wasn't executed, we skip using it.
        if testObj.predictionLevel.value != predictionLevels.NO_TEST and\
          testObj.predictionLevel.value != predictionLevels.DISABLED:
          sum += testObj.predictionLevel.value
          executedTstCnt += 1

      if executedTstCnt:
        self._ensemblePrediction.value = int(round(sum / float(executedTstCnt)))


    if self.logger is not None:
      self.logger.debug("Overall Prediction: %d(%s)" %(self._ensemblePrediction.value, str(self._ensemblePrediction)))
    return self._ensemblePrediction


