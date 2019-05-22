from blackmagic import db
from blackmagic import segaux
from collections import namedtuple
from cytoolz import assoc
from cytoolz import first
from cytoolz import get

import blackmagic
import numpy
import test


def test_independent_2d():
    inputs = numpy.array([[1, 2, 3],
                          [11, 22, 33],
                          [111, 222, 333]])

    expected = numpy.array([[2, 3], [22, 33], [222, 333]])
    
    outputs = segaux.independent(inputs)
    
    assert numpy.array_equal(expected, outputs)

    
def test_independent_1d():
    inputs = numpy.array([1, 2, 3, 4])

    expected = numpy.array([2, 3, 4])

    outputs = segaux.independent(inputs)

    assert numpy.array_equal(expected, outputs)
    

def test_dependent_2d():
    inputs = numpy.array([[1, 2, 3],
                          [11, 22, 33],
                          [111, 222, 333]])

    expected = numpy.array([1, 11, 111])

    outputs = segaux.dependent(inputs)
    
    assert numpy.array_equal(expected, outputs)


def test_dependent_1d():
    inputs = numpy.array([1, 2, 3, 4])
                          
    expected = numpy.array([1])

    outputs = segaux.dependent(inputs)
    
    assert numpy.array_equal(expected, outputs)


def test_aux():
    inputs = {'cx': test.cx,
              'cy': test.cy,
              'acquired': test.acquired}

    outputs = segaux.aux(inputs, blackmagic.cfg)
    
    assert get('aux', outputs, None) is not None


def test_aux_filter():
    inputs = {'aux': {(1,): {'nlcdtrn': [0]},
                      (2,): {'nlcdtrn': [1]},
                      (3,): {'nlcdtrn': [2]}}}

    expected = {'aux': {(2,): {'nlcdtrn': [1]},
                        (3,): {'nlcdtrn': [2]}}}

    outputs = segaux.aux_filter(inputs)

    assert expected == outputs
    
    
def test_segments():
    inputs = {'cx': test.cx,
              'cy': test.cy,
              'cluster': db.cluster(blackmagic.cfg)}

    outputs = segaux.segments(inputs, blackmagic.cfg)

    segments = get('segments', outputs, None)

    assert segments is not None
    assert len(segments) > 0


def test_combine():
    seg = namedtuple('Segment', ['cx', 'cy', 'px', 'py', 'segval'])
    key = namedtuple('Key', ['cx', 'cy', 'px', 'py'])
    
    inputs = {'segments': [seg(1, 2, 3, 4, 5),
                           seg(2, 3, 4, 5, 6),
                           seg(3, 4, 5, 6, 7)],
              'aux': {key(1, 2, 3, 4): {'auxval': 111},
                      key(2, 3, 4, 5): {'auxval': 222},
                      key(3, 4, 5, 6): {'auxval': 333}}}

    expected = {'segments': [seg(1, 2, 3, 4, 5),
                             seg(2, 3, 4, 5, 6),
                             seg(3, 4, 5, 6, 7)],
                'aux':{key(1, 2, 3, 4): {'auxval': 111},
                       key(2, 3, 4, 5): {'auxval': 222},
                       key(3, 4, 5, 6): {'auxval': 333}},
                'data': [{'cx': 1, 'cy': 2, 'px': 3, 'py': 4, 'segval': 5, 'auxval': 111},
                         {'cx': 2, 'cy': 3, 'px': 4, 'py': 5, 'segval': 6, 'auxval': 222},
                         {'cx': 3, 'cy': 4, 'px': 5, 'py': 6, 'segval': 7, 'auxval': 333}]}
                         
    assert expected == segaux.combine(inputs)
    
  
def test_prediction_date_fn():
    inputs = {'sday' : '1980-01-01',
              'eday' : '1986-06-01',
              'month': '07',
              'day'  : '01'}

    expected = ['1980-07-01',
                '1981-07-01',
                '1982-07-01',
                '1983-07-01',
                '1984-07-01',
                '1985-07-01']

    outputs = segaux.prediction_date_fn(**inputs)

    assert expected == outputs

    
    inputs = {'sday' : '0001-01-01',
              'eday' : '0002-11-01',
              'month': '07',
              'day'  : '01'}

    expected = ['0001-07-01',
                '0002-07-01']

    outputs = segaux.prediction_date_fn(**inputs)

    assert expected == outputs

    
def test_default_prediction_date():
    assert '0001-01-01' == segaux.default_prediction_date({'sday': '0001-01-01',
                                                           'eday': '0001-01-01'})

    assert segaux.default_prediction_date({'sday': '0001-01-02',
                                           'eday': '0001-01-03'}) is None
    
    
def test_prediction_dates():
    inputs = {'segments' : [{'sday': '1980-01-01',
                             'eday': '1986-06-01'},
                            {'sday': '0001-01-01',
                             'eday': '0001-01-01'}],
              'month': '07',
              'day'  : '01'}

    expected = [{'sday': '1980-01-01',
                 'eday': '1986-06-01',
                 'date': '1980-07-01'},
                {'sday': '1980-01-01',
                 'eday': '1986-06-01',
                 'date': '1981-07-01'},
                {'sday': '1980-01-01',
                 'eday': '1986-06-01',
                 'date': '1982-07-01'},
                {'sday': '1980-01-01',
                 'eday': '1986-06-01',
                 'date': '1983-07-01'},
                {'sday': '1980-01-01',
                 'eday': '1986-06-01',
                 'date': '1984-07-01'},
                {'sday': '1980-01-01',
                 'eday': '1986-06-01',
                 'date': '1985-07-01'},
                {'sday': '0001-01-01',
                 'eday': '0001-01-01',
                 'date': '0001-01-01'}]
               
    outputs = list(segaux.prediction_dates(**inputs))

    assert expected == outputs


def test_training_date():
    inputs = {'date': '1980-01-01',
              'data': {'a': 1}}

    expected = {'a': 1,
                'date': '1980-01-01'}

    outputs = segaux.training_date(**inputs)

    assert outputs == expected
    
    
def test_add_training_dates():
    inputs = {'date': '1979-06-07',
              'data': [{'a': 1}, {'b': 2}]}

    expected = {'date': '1979-06-07',
                'data': [{'a': 1, 'date': '1979-06-07'},
                         {'b': 2, 'date': '1979-06-07'}]}

    outputs = segaux.add_training_dates(inputs)

    assert expected == outputs


def test_average_reflectance_fn():

    segment = {'slope': [1],
               'blint': 2,
               'grint': 2,
               'niint': 2,
               'reint': 2,
               's1int': 2,
               's2int': 2,
               'thint': 2,
               'date': '1980-01-01'} # ordinal day 722815
    
    expected = {'slope': [1],
                'blint': 2,
                'grint': 2,
                'niint': 2,
                'reint': 2,
                's1int': 2,
                's2int': 2,
                'thint': 2,
                'blar': 722817,
                'grar': 722817,
                'niar': 722817,
                'rear': 722817,
                's1ar': 722817,
                's2ar': 722817,
                'thar': 722817,
                'date': '1980-01-01'}
    
    outputs = segaux.average_reflectance_fn(segment)

    assert expected == outputs 
              

def test_average_reflectance():
    inputs = [{'slope': [1],
               'blint': 2,
               'grint': 2,
               'niint': 2,
               'reint': 2,
               's1int': 2,
               's2int': 2,
               'thint': 2,
               'date' : '1980-01-01'},
              {'slope': [1],
               'blint': 2,
               'grint': 2,
               'niint': 2,
               'reint': 2,
               's1int': 2,
               's2int': 2,
               'thint': 2,
               'date' : '1982-12-31'}]

    expected = [{'slope': [1],
                 'blint': 2,
                 'grint': 2,
                 'niint': 2,
                 'reint': 2,
                 's1int': 2,
                 's2int': 2,
                 'thint': 2,
                 'blar': 722817,
                 'grar': 722817,
                 'niar': 722817,
                 'rear': 722817,
                 's1ar': 722817,
                 's2ar': 722817,
                 'thar': 722817,
                 'date': '1980-01-01'},
                {'slope': [1],
                 'blint': 2,
                 'grint': 2,
                 'niint': 2,
                 'reint': 2,
                 's1int': 2,
                 's2int': 2,
                 'thint': 2,
                 'blar': 723912,
                 'grar': 723912,
                 'niar': 723912,
                 'rear': 723912,
                 's1ar': 723912,
                 's2ar': 723912,
                 'thar': 723912,
                 'date': '1982-12-31'}]

    outputs = list(segaux.average_reflectance(inputs))
                 
    assert expected == outputs
    

def test_unload_segments():
    inputs = {'segments': 1, 'other': 2}

    outputs = segaux.unload_segments(inputs)

    assert get('segments', outputs, None) is None
    assert get('other', outputs) is 2


def test_unload_aux():
    inputs = {'aux': 1, 'other': 2}
    outputs = segaux.unload_aux(inputs)
    assert get('aux', outputs, None) is None
    assert get('other', outputs) is 2


def test_log_chip():
    inputs = {'tx': 1,
              'ty': 2,
              'cx': 3,
              'cy': 4,
              'date': '1980-01-01',
              'acquired': '1980/2019'}

    assert segaux.log_chip(inputs) == inputs
    

def test_exit_pipeline():
    inputs = {'data': 1}

    assert segaux.exit_pipeline(inputs) == 1

    
def test_to_numpy_1d():

    inputs = [1, 2, 3, 4]

    expected = numpy.array(inputs, dtype=numpy.float32)

    outputs = segaux.to_numpy(inputs)

    assert numpy.array_equal(expected, outputs)

    
def test_to_numpy_2d():

    inputs = [[1, 2, 3], [4, 5, 6]]

    expected = numpy.array(inputs, dtype=numpy.float32)

    outputs = segaux.to_numpy(inputs)

    assert numpy.array_equal(expected, outputs)


def test_standard_format():
    inputs = {'nlcdtrn': [1],
              'aspect' : [2],
              'posidex': [3],
              'slope'  : [4],
              'mpw'    : [5],
              'dem'    : [6],
              'blcoef' : [7],
              'blrmse' : 8,
              'blar'   : 9 ,
              'grcoef' : [10],
              'grrmse' : 11,
              'grar'   : 12,
              'nicoef' : [13],
              'nirmse' : 14,
              'niar'   : 15,
              'recoef' : [16],
              'rermse' : 17,
              'rear'   : 18,
              's1coef' : [19],
              's1rmse' : 20,
              's1ar'   : 21,
              's2coef' : [22],
              's2rmse' : 23,
              's2ar'   : 24,
              'thcoef' : [25],
              'thrmse' : 26,
              'thar'   : 27,
              'some'   : 28,
              'extra'  : 29,
              'values' : 30}

    expected = [1, 2, 3, 4, 5, 6, 7, 8, 9,
                10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 25, 26, 27]
            
    outputs = segaux.standard_format(inputs)

    assert expected == outputs


def test_training_format():
    inputs = {'data': [{'nlcdtrn': [1],
                        'aspect' : [2],
                        'posidex': [3],
                        'slope'  : [4],
                        'mpw'    : [5],
                        'dem'    : [6],
                        'blcoef' : [7],
                        'blrmse' : 8,
                        'blar'   : 9 ,
                        'grcoef' : [10],
                        'grrmse' : 11,
                        'grar'   : 12,
                        'nicoef' : [13],
                        'nirmse' : 14,
                        'niar'   : 15,
                        'recoef' : [16],
                        'rermse' : 17,
                        'rear'   : 18,
                        's1coef' : [19],
                        's1rmse' : 20,
                        's1ar'   : 21,
                        's2coef' : [22],
                        's2rmse' : 23,
                        's2ar'   : 24,
                        'thcoef' : [25],
                        'thrmse' : 26,
                        'thar'   : 27,
                        'some'   : 28,
                        'extra'  : 29,
                        'values' : 30}]}

    expected = {'data': numpy.array([[1., 2., 3., 4., 5., 6., 7., 8., 9.,
                                      10., 11., 12., 13., 14., 15., 16., 17., 18.,
                                      19., 20., 21., 22., 23., 24., 25., 26., 27.]],
                                    dtype=numpy.float32)}

    outputs = segaux.training_format(inputs)

    assert numpy.array_equal(get('data', expected),
                             get('data', outputs))


def test_prediction_format():
    inputs = {'cx': 100,
              'cy': 111,
              'px': 112,
              'py': 113,
              'sday': 114,
              'eday': 115,
              'date': 116,
              'nlcdtrn': [1],
              'aspect' : [2],
              'posidex': [3],
              'slope'  : [4],
              'mpw'    : [5],
              'dem'    : [6],
              'blcoef' : [7],
              'blrmse' : 8,
              'blar'   : 9 ,
              'grcoef' : [10],
              'grrmse' : 11,
              'grar'   : 12,
              'nicoef' : [13],
              'nirmse' : 14,
              'niar'   : 15,
              'recoef' : [16],
              'rermse' : 17,
              'rear'   : 18,
              's1coef' : [19],
              's1rmse' : 20,
              's1ar'   : 21,
              's2coef' : [22],
              's2rmse' : 23,
              's2ar'   : 24,
              'thcoef' : [25],
              'thrmse' : 26,
              'thar'   : 27,
              'some'   : 28,
              'extra'  : 29,
              'values' : 30}

    expected = {'cx'  : 100,
                'cy'  : 111,
                'px'  : 112,
                'py'  : 113,
                'sday': 114,
                'eday': 115,
                'date': 116,
                'independent': numpy.array([2, 3, 4, 5, 6, 7, 8, 9, 10,
                                            11, 12, 13, 14, 15, 16, 17,
                                            18, 19, 20, 21, 22, 23, 24,
                                            25, 26, 27], dtype=numpy.float32)}

    outputs = segaux.prediction_format(inputs)

    assert len(expected) == len(outputs)
    
    assert numpy.array_equal(get('independent', expected), 
                             get('independent', outputs))
    
    expected.pop('independent')
    outputs.pop('independent')

    assert expected == outputs
