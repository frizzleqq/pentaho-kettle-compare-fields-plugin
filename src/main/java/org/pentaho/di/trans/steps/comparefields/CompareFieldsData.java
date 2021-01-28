package org.pentaho.di.trans.steps.comparefields;

/**
 * 
 */

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * @author matt
 *
 */
public class CompareFieldsData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public RowSet identicalRowSet = null;
  public RowSet changedRowSet = null;
  public RowSet addedRowSet = null;
  public RowSet removedRowSet = null;

}
