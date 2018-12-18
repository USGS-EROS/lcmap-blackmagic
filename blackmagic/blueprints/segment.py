from blackmagic import cfg
from blackmagic import db
from cytoolz import first
from cytoolz import get
from cytoolz import get_in
from cytoolz import second
from datetime import date
import ccd
import logging

logger = logging.getLogger('blackmagic.changedetection')

def saveccd(detection, q):
    q.put(db.insert_chip(cfg, detection))
    q.put(db.insert_pixel(cfg, detection))
    q.put(db.insert_segment(cfg, detection))
    return detection


def default(change_models):
    # if there are no change models, append an empty one to
    # signify that ccd was run for the point, setting start_day and end_day to day 1

    return [{'start_day': 1, 'end_day': 1, 'break_day': 1}] if not change_models else change_models


def coefficients(change_model, spectra):
    coefs = get_in([spectra, 'coefficients'], change_model, None)
    return list(coefs) if coefs else None


def format(cx, cy, px, py, dates, ccdresult):
    logger.debug('formatting {},{},{},{}'.format(cx, cy, px, py))
    return [
             {'cx'     : int(cx),
              'cy'     : int(cy),
              'px'     : int(px),
              'py'     : int(py),
              'sday'   : date.fromordinal(get('start_day', cm)).isoformat(),
              'eday'   : date.fromordinal(get('end_day', cm)).isoformat(),
              'bday'   : date.fromordinal(get('break_day', cm, None)).isoformat(),
              'chprob' : get('change_probability', cm, None),
              'curqa'  : get('curve_qa', cm, None),
              'blmag'  : get_in(['blue', 'magnitude'], cm, None),
              'grmag'  : get_in(['green', 'magnitude'], cm, None),
              'remag'  : get_in(['red', 'magnitude'], cm, None),
              'nimag'  : get_in(['nir', 'magnitude'], cm, None),
              's1mag'  : get_in(['swir1', 'magnitude'], cm, None),
              's2mag'  : get_in(['swir2', 'magnitude'], cm, None),
              'thmag'  : get_in(['thermal', 'magnitude'], cm, None),
              'blrmse' : get_in(['blue', 'rmse'], cm, None),
              'grrmse' : get_in(['green', 'rmse'], cm, None),
              'rermse' : get_in(['red', 'rmse'], cm, None),
              'nirmse' : get_in(['nir', 'rmse'], cm, None),
              's1rmse' : get_in(['swir1', 'rmse'], cm, None),
              's2rmse' : get_in(['swir2', 'rmse'], cm, None),
              'thrmse' : get_in(['thermal', 'rmse'], cm, None),
              'blcoef' : coefficients(cm, 'blue'),
              'grcoef' : coefficients(cm, 'green'),
              'recoef' : coefficients(cm, 'red'),
              'nicoef' : coefficients(cm, 'nir'),
              's1coef' : coefficients(cm, 'swir1'),
              's2coef' : coefficients(cm, 'swir2'),
              'thcoef' : coefficients(cm, 'thermal'),
              'blint'  : get_in(['blue', 'intercept'], cm, None),
              'grint'  : get_in(['green', 'intercept'], cm, None),
              'reint'  : get_in(['red', 'intercept'], cm, None),
              'niint'  : get_in(['nir', 'intercept'], cm, None),
              's1int'  : get_in(['swir1', 'intercept'], cm, None),
              's2int'  : get_in(['swir2', 'intercept'], cm, None),
              'thint'  : get_in(['thermal', 'intercept'], cm, None),
              'dates'  : [date.fromordinal(o).isoformat() for o in dates],
              'mask'   : get('processing_mask', ccdresult, None)}
             for cm in default(get('change_models', ccdresult, None))]


def detect(timeseries):
   
    cx, cy, px, py = first(timeseries)

    return format(cx=cx,
                  cy=cy,
                  px=px,
                  py=py,
                  dates=get('dates', second(timeseries)),
                  ccdresult=ccd.detect(**second(timeseries)))


def pipeline(x, q):
    return count(map(partial(saveccd, q=q), detect(x)))


def delete_detections(timeseries):
    cx, cy, _, _ = first(first(timeseries))
    try:
        x = int(cx)
        y = int(cy)
        _ = db.execute(cfg, db.delete_chip(cfg, x, y))
        _ = db.execute(cfg, db.delete_pixel(cfg, x, y))
        _ = db.execute(cfg, db.delete_segment(cfg, x, y))
        
    except Exception as e:
        logger.exception('Exception deleting partition for x:{cx} y:{cy}'.format(cx=x, cy=y))

    return timeseries
