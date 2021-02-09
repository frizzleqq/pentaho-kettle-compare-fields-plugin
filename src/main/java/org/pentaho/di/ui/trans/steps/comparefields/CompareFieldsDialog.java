/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.ui.trans.steps.comparefields;

import java.util.Arrays;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.comparefields.CompareField;
import org.pentaho.di.trans.steps.comparefields.CompareFieldsMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class CompareFieldsDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = CompareFieldsMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlFields;
  private TableView wFields;

  private Label wlIdenticalTarget;
  private CCombo wIdenticalTarget;
  private Label wlChangedTarget;
  private CCombo wChangedTarget;
  private Label wlAddedTarget;
  private CCombo wAddedTarget;
  private Label wlRemovedTarget;
  private CCombo wRemovedTarget;

  private Label wlAddChangedFields;
  private Button wAddChangedFields;

  private Label wlFieldsListField;
  private Text wFieldsListField;

  private CompareFieldsMeta input;

  private RowMetaInterface inputFields;
  private String[] inputFieldNames;
  private String[] outputStepNames;

  public CompareFieldsDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (CompareFieldsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // TODO: grab field list in thread in the background...
    //
    try {
      inputFields = transMeta.getPrevStepFields( stepMeta );
    } catch ( KettleStepException ex ) {
      inputFields = new RowMeta();
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "CompareFieldsDialog.Exception.CantGetFieldsFromPreviousSteps.Title" ),
        BaseMessages.getString( PKG, "CompareFieldsDialog.Exception.CantGetFieldsFromPreviousSteps.Message" ), ex );
    }
    inputFieldNames = inputFields.getFieldNames();
    Arrays.sort( inputFieldNames );

    // TODO: grab field list in thread in the background...
    //
    outputStepNames = transMeta.getNextStepNames( stepMeta );
    Arrays.sort( outputStepNames );

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, margin );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    wlIdenticalTarget = new Label( shell, SWT.RIGHT );
    wlIdenticalTarget.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.IdenticalTarget.Label" ) );
    props.setLook( wlIdenticalTarget );
    FormData fdlIdenticalTarget = new FormData();
    fdlIdenticalTarget.left = new FormAttachment( 0, 0 );
    fdlIdenticalTarget.right = new FormAttachment( middle, 0 );
    fdlIdenticalTarget.top = new FormAttachment( lastControl, margin );
    wlIdenticalTarget.setLayoutData( fdlIdenticalTarget );
    wIdenticalTarget = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wIdenticalTarget );
    FormData fdIdenticalTarget = new FormData();
    fdIdenticalTarget.left = new FormAttachment( middle, margin );
    fdIdenticalTarget.right = new FormAttachment( 100, 0 );
    fdIdenticalTarget.top = new FormAttachment( lastControl, margin );
    wIdenticalTarget.setLayoutData( fdIdenticalTarget );
    wIdenticalTarget.setItems( outputStepNames );
    lastControl = wIdenticalTarget;

    wlChangedTarget = new Label( shell, SWT.RIGHT );
    wlChangedTarget.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.ChangedTarget.Label" ) );
    props.setLook( wlChangedTarget );
    FormData fdlChangedTarget = new FormData();
    fdlChangedTarget.left = new FormAttachment( 0, 0 );
    fdlChangedTarget.right = new FormAttachment( middle, 0 );
    fdlChangedTarget.top = new FormAttachment( lastControl, margin );
    wlChangedTarget.setLayoutData( fdlChangedTarget );
    wChangedTarget = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wChangedTarget );
    FormData fdChangedTarget = new FormData();
    fdChangedTarget.left = new FormAttachment( middle, margin );
    fdChangedTarget.right = new FormAttachment( 100, 0 );
    fdChangedTarget.top = new FormAttachment( lastControl, margin );
    wChangedTarget.setLayoutData( fdChangedTarget );
    wChangedTarget.setItems( outputStepNames );
    lastControl = wChangedTarget;

    wlAddedTarget = new Label( shell, SWT.RIGHT );
    wlAddedTarget.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.AddedTarget.Label" ) );
    props.setLook( wlAddedTarget );
    FormData fdlAddedTarget = new FormData();
    fdlAddedTarget.left = new FormAttachment( 0, 0 );
    fdlAddedTarget.right = new FormAttachment( middle, 0 );
    fdlAddedTarget.top = new FormAttachment( lastControl, margin );
    wlAddedTarget.setLayoutData( fdlAddedTarget );
    wAddedTarget = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAddedTarget );
    FormData fdAddedTarget = new FormData();
    fdAddedTarget.left = new FormAttachment( middle, margin );
    fdAddedTarget.right = new FormAttachment( 100, 0 );
    fdAddedTarget.top = new FormAttachment( lastControl, margin );
    wAddedTarget.setLayoutData( fdAddedTarget );
    wAddedTarget.setItems( outputStepNames );
    lastControl = wAddedTarget;

    wlRemovedTarget = new Label( shell, SWT.RIGHT );
    wlRemovedTarget.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.RemovedTarget.Label" ) );
    props.setLook( wlRemovedTarget );
    FormData fdlRemovedTarget = new FormData();
    fdlRemovedTarget.left = new FormAttachment( 0, 0 );
    fdlRemovedTarget.right = new FormAttachment( middle, 0 );
    fdlRemovedTarget.top = new FormAttachment( lastControl, margin );
    wlRemovedTarget.setLayoutData( fdlRemovedTarget );
    wRemovedTarget = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRemovedTarget );
    FormData fdRemovedTarget = new FormData();
    fdRemovedTarget.left = new FormAttachment( middle, margin );
    fdRemovedTarget.right = new FormAttachment( 100, 0 );
    fdRemovedTarget.top = new FormAttachment( lastControl, margin );
    wRemovedTarget.setLayoutData( fdRemovedTarget );
    wRemovedTarget.setItems( outputStepNames );
    lastControl = wRemovedTarget;

    wlAddChangedFields = new Label( shell, SWT.RIGHT );
    wlAddChangedFields.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.AddChangedFields.Label" ) );
    props.setLook( wlAddChangedFields );
    FormData fdlAddChangedFields = new FormData();
    fdlAddChangedFields.left = new FormAttachment( 0, 0 );
    fdlAddChangedFields.right = new FormAttachment( middle, 0 );
    fdlAddChangedFields.top = new FormAttachment( lastControl, margin );
    wlAddChangedFields.setLayoutData( fdlAddChangedFields );
    wAddChangedFields = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wAddChangedFields );
    FormData fdAddChangedFields = new FormData();
    fdAddChangedFields.left = new FormAttachment( middle, margin );
    fdAddChangedFields.right = new FormAttachment( 100, 0 );
    fdAddChangedFields.top = new FormAttachment( lastControl, margin );
    wAddChangedFields.setLayoutData( fdAddChangedFields );
    lastControl = wAddChangedFields;
    wAddChangedFields.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        enableWidgets();
      }
    } );

    wlFieldsListField = new Label( shell, SWT.RIGHT );
    wlFieldsListField.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.FieldsListField.Label" ) );
    props.setLook( wlFieldsListField );
    FormData fdlFieldsListField = new FormData();
    fdlFieldsListField.left = new FormAttachment( 0, 0 );
    fdlFieldsListField.right = new FormAttachment( middle, 0 );
    fdlFieldsListField.top = new FormAttachment( lastControl, margin );
    wlFieldsListField.setLayoutData( fdlFieldsListField );
    wFieldsListField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFieldsListField );
    FormData fdFieldsListField = new FormData();
    fdFieldsListField.left = new FormAttachment( middle, margin );
    fdFieldsListField.right = new FormAttachment( 100, 0 );
    fdFieldsListField.top = new FormAttachment( lastControl, margin );
    wFieldsListField.setLayoutData( fdFieldsListField );
    lastControl = wFieldsListField;

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    // The name of the field to validate
    //
    wlFields = new Label( shell, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment( 0, 0 );
    fdlFieldName.right = new FormAttachment( middle, 0 );
    fdlFieldName.top = new FormAttachment( lastControl, margin );
    wlFields.setLayoutData( fdlFieldName );

    ColumnInfo[] fieldColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "CompareFieldsDialog.ReferenceField.Label" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, inputFieldNames ),
      new ColumnInfo( BaseMessages.getString( PKG, "CompareFieldsDialog.CompareField.Label" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, inputFieldNames ),
    };
    wFields = new TableView( transMeta, shell,
      SWT.MULTI | SWT.LEFT | SWT.BORDER, fieldColumns, input.getCompareFields().size(),
      lsMod, props );
    props.setLook( wFields );
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment( middle, margin );
    fdFieldName.right = new FormAttachment( 100, 0 );
    fdFieldName.top = new FormAttachment( lastControl, margin );
    fdFieldName.bottom = new FormAttachment( wOK, -margin * 2 );
    wFields.setLayoutData( fdFieldName );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wIdenticalTarget.addSelectionListener( lsDef );
    wChangedTarget.addSelectionListener( lsDef );
    wAddedTarget.addSelectionListener( lsDef );
    wRemovedTarget.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( backupChanged );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected void enableWidgets() {
    boolean addingList = wAddChangedFields.getSelection();

    wlFieldsListField.setEnabled( addingList );
    wFieldsListField.setEnabled( addingList );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wIdenticalTarget.setText( getStepname( input.getIdenticalTargetStepMeta() ) );
    wChangedTarget.setText( getStepname( input.getChangedTargetStepMeta() ) );
    wAddedTarget.setText( getStepname( input.getAddedTargetStepMeta() ) );
    wRemovedTarget.setText( getStepname( input.getRemovedTargetStepMeta() ) );

    wAddChangedFields.setSelection( input.isAddingFieldsList() );
    wFieldsListField.setText( Const.NVL( input.getFieldsListFieldname(), "" ) );

    for ( int i = 0; i < input.getCompareFields().size(); i++ ) {
      TableItem item = wFields.table.getItem( i );
      CompareField field = input.getCompareFields().get( i );
      if ( field != null ) {
        item.setText( 1, Const.NVL( field.getReferenceFieldname(), "" ) );
        item.setText( 2, Const.NVL( field.getCompareFieldname(), "" ) );
      }
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    enableWidgets();

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private String getStepname( StepMeta stepMeta ) {
    if ( stepMeta == null ) {
      return "";
    }
    return Const.NVL( stepMeta.getName(), "" );
  }

  private void cancel() {
    stepname = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    input.setIdenticalTargetStepMeta( findStep( wIdenticalTarget.getText() ) );
    input.setChangedTargetStepMeta( findStep( wChangedTarget.getText() ) );
    input.setAddedTargetStepMeta( findStep( wAddedTarget.getText() ) );
    input.setRemovedTargetStepMeta( findStep( wRemovedTarget.getText() ) );

    input.setAddingFieldsList( wAddChangedFields.getSelection() );
    input.setFieldsListFieldname( wFieldsListField.getText() );

    int nrValues = wFields.nrNonEmpty();
    input.getCompareFields().clear();

    for ( int i = 0; i < nrValues; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      String referenceFieldname = item.getText( 1 );
      String compareFieldname = item.getText( 2 );
      CompareField compareField = new CompareField( referenceFieldname, compareFieldname );
      input.getCompareFields().add( compareField );
    }

    stepname = wStepname.getText(); // return value

    dispose();
  }

  private StepMeta findStep( String stepName ) {
    return transMeta.findStep( stepName );
  }
}
