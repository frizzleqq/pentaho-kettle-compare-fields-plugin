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

package org.apache.hop.pipeline.transforms.comparefields;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.Arrays;

public class CompareFieldsDialog extends BaseTransformDialog implements ITransformDialog {
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

  private String[] inputFieldNames;
  private String[] outputStepNames;

  public CompareFieldsDialog(Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname );
    input = (CompareFieldsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    IRowMeta inputFields;
    try {
      inputFields = pipelineMeta.getPrevTransformFields( variables, transformName );
    } catch (HopTransformException ex ) {
      inputFields = new RowMeta();
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "CompareFieldsDialog.Exception.CantGetFieldsFromPreviousSteps.Title" ),
        BaseMessages.getString( PKG, "CompareFieldsDialog.Exception.CantGetFieldsFromPreviousSteps.Message" ), ex );
    }
    inputFieldNames = inputFields.getFieldNames();
    Arrays.sort( inputFieldNames );

    // TODO: grab field list in thread in the background...
    //
    outputStepNames = pipelineMeta.getNextTransformNames( transformMeta );
    Arrays.sort( outputStepNames );

    // Stepname line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "CompareFieldsDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, margin );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

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
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

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
    wFields = new TableView( variables, shell,
      SWT.MULTI | SWT.LEFT | SWT.BORDER, fieldColumns, input.getCompareFields().size(),
      lsMod, props );
    props.setLook( wFields );
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment( middle, margin );
    fdFieldName.right = new FormAttachment( 100, 0 );
    fdFieldName.top = new FormAttachment( lastControl, margin );
    fdFieldName.bottom = new FormAttachment( wOk, -margin * 2 );
    wFields.setLayoutData( fdFieldName );

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
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
    return transformName;
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
    wIdenticalTarget.setText( getTransformName( input.getIdenticalTargetTransformMeta() ) );
    wChangedTarget.setText( getTransformName( input.getChangedTargetTransformMeta() ) );
    wAddedTarget.setText( getTransformName( input.getAddedTargetTransformMeta() ) );
    wRemovedTarget.setText( getTransformName( input.getRemovedTargetTransformMeta() ) );

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

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private String getTransformName(TransformMeta stepMeta ) {
    if ( stepMeta == null ) {
      return "";
    }
    return Const.NVL( stepMeta.getName(), "" );
  }

  private void cancel() {
    transformMeta = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    input.setIdenticalTargetTransformMeta( pipelineMeta.findTransform( wIdenticalTarget.getText() ) );
    input.setChangedTargetTransformMeta( pipelineMeta.findTransform( wChangedTarget.getText() ) );
    input.setAddedTargetTransformMeta( pipelineMeta.findTransform( wAddedTarget.getText() ) );
    input.setRemovedTargetTransformMeta( pipelineMeta.findTransform( wRemovedTarget.getText() ) );

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

    transformName = wTransformName.getText(); // return value

    dispose();
  }
}
