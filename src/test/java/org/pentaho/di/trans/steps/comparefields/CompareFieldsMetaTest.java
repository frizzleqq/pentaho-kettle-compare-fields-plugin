package org.pentaho.di.trans.steps.comparefields;

import com.google.common.collect.ImmutableList;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.BooleanLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CompareFieldsMetaTest {
    Class<CompareFieldsMeta> testMetaClass = CompareFieldsMeta.class;
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

    @Test
    public void testLoadSave() throws KettleException {
        List<String> attributes =
                Arrays.asList( "identical_target_step", "changed_target_step", "added_target_step",
                        "removed_target_step", "add_fields_list", "fields_list_field");

        Map<String, String> getterMap = new HashMap<String, String>();
        Map<String, String> setterMap = new HashMap<String, String>();

        Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
        attrValidatorMap.put( "identicalTargetStepMeta", new StringLoadSaveValidator() );
        attrValidatorMap.put( "changedTargetStepMeta", new StringLoadSaveValidator() );
        attrValidatorMap.put( "addedTargetStepMeta", new StringLoadSaveValidator() );
        attrValidatorMap.put( "removedTargetStepMeta", new StringLoadSaveValidator() );
        attrValidatorMap.put( "addingFieldsList", new BooleanLoadSaveValidator() );
        attrValidatorMap.put( "fieldsListFieldname", new StringLoadSaveValidator() );

        getterMap.put( "identical_target_step", "getIdenticalTargetStepname" );
        getterMap.put( "changed_target_step", "getChangedTargetStepname" );
        getterMap.put( "added_target_step", "getAddedTargetStepname" );
        getterMap.put( "removed_target_step", "getRemovedTargetStepname" );
        getterMap.put( "add_fields_list", "isAddingFieldsList" );
        getterMap.put( "fields_list_field", "getFieldsListFieldname" );

        setterMap.put( "identical_target_step", "setIdenticalTargetStepname" );
        setterMap.put( "changed_target_step", "setChangedTargetStepname" );
        setterMap.put( "added_target_step", "setAddedTargetStepname" );
        setterMap.put( "removed_target_step", "setRemovedTargetStepname" );
        setterMap.put( "add_fields_list", "setAddingFieldsList" );
        setterMap.put( "fields_list_field", "setFieldsListFieldname" );

        Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

        /*
         * If custom object types are used, additional arguments may need to be passed to the LoadSaveTester.
         * These are typically custom implementations of the FieldLoadSaveValidator interface for each Object type.
         */
        LoadSaveTester<CompareFieldsMeta> loadSaveTester = new LoadSaveTester( testMetaClass, attributes, getterMap,
                setterMap, attrValidatorMap, typeValidatorMap );

        /*
         * Test the XML and Repository serialization of this StepMeta.
         * Also tests the clone() method for the Step Meta, which is used in distributed environments (e.g. Carte)
         */
        loadSaveTester.testSerialization();
    }


    @Test
    public void testGetStepData() {
        CompareFieldsMeta m = new CompareFieldsMeta();
        assertEquals( CompareFieldsData.class, m.getStepData().getClass() );
    }

    @Test
    public void testDefaults() throws KettleStepException {
        CompareFieldsMeta m = new CompareFieldsMeta();
        m.setDefault();

        RowMetaInterface rowMeta = new RowMeta();
        m.getFields( rowMeta, "foobar", null, null, null, null, null );

        assertEquals( rowMeta.size(), 0 );
    }


    @Test
    public void testGetFieldsEmpty() throws KettleStepException {
        CompareFieldsMeta meta = new CompareFieldsMeta();

        RowMeta inputRow = new RowMeta();
        StepMeta stepMeta = new StepMeta( "Compare", meta );

        meta.getFields( inputRow, "Compare Fields",
                new RowMetaInterface[]{ inputRow }, stepMeta, new Variables(), null, null );

        assertNotNull( inputRow );
        assertTrue( inputRow.isEmpty() );
        assertEquals( 0, inputRow.size() );
    }

    @Test
    public void testGetFieldsAddWithoutChangedFieldname() throws KettleStepException {
        CompareFieldsMeta meta = new CompareFieldsMeta();
        meta.setAddingFieldsList(true);

        RowMeta inputRow = new RowMeta();
        StepMeta stepMeta = new StepMeta( "Compare", meta );

        meta.getFields( inputRow, "Compare Fields",
                new RowMetaInterface[]{ inputRow }, stepMeta, new Variables(), null, null );

        assertNotNull( inputRow );
        assertTrue( inputRow.isEmpty() );
        assertEquals( 0, inputRow.size() );
    }

    @Test
    public void testGetFieldsDisabledChangedFieldname() throws KettleStepException {
        CompareFieldsMeta meta = new CompareFieldsMeta();
        meta.setFieldsListFieldname("changed");

        RowMeta inputRow = new RowMeta();
        StepMeta stepMeta = new StepMeta( "Compare", meta );

        meta.getFields( inputRow, "Compare Fields",
                new RowMetaInterface[]{ inputRow }, stepMeta, new Variables(), null, null );

        assertNotNull( inputRow );
        assertTrue( inputRow.isEmpty() );
        assertEquals( 0, inputRow.size() );
    }

    @Test
    public void testGetFieldsAddChangedFieldname() throws KettleStepException {
        CompareFieldsMeta meta = new CompareFieldsMeta();
        meta.setAddingFieldsList(true);
        meta.setFieldsListFieldname("changed");

        RowMeta inputRow = new RowMeta();
        StepMeta stepMeta = new StepMeta( "Compare", meta );

        meta.getFields( inputRow, "Compare Fields",
                new RowMetaInterface[]{ inputRow }, stepMeta, new Variables(), null, null );

        assertNotNull( inputRow );
        assertFalse( inputRow.isEmpty() );
        assertEquals( 1, inputRow.size() );
        assertEquals( "changed", inputRow.getValueMeta( 0 ).getName() );
    }

    @Test
    public void testModifiedTarget() {
        CompareFieldsMeta compareFieldsMeta = new CompareFieldsMeta();
        StepMeta identicalOutput = new StepMeta( "identical", new DummyTransMeta() );
        StepMeta changedOutput = new StepMeta( "changed", new DummyTransMeta() );
        StepMeta addedOutput = new StepMeta( "added", new DummyTransMeta() );
        StepMeta removedOutput = new StepMeta( "removed", new DummyTransMeta() );

        compareFieldsMeta.setIdenticalTargetStepname(identicalOutput.getName());
        compareFieldsMeta.setChangedTargetStepname(changedOutput.getName());
        compareFieldsMeta.setAddedTargetStepname(addedOutput.getName());
        compareFieldsMeta.setRemovedTargetStepname(removedOutput.getName());
        compareFieldsMeta.searchInfoAndTargetSteps(
                ImmutableList.of(identicalOutput, changedOutput, addedOutput, removedOutput ) );

        identicalOutput.setName( "identical renamed" );
        changedOutput.setName( "changed renamed" );
        addedOutput.setName( "added renamed" );
        removedOutput.setName( "removed renamed" );

        assertEquals( "identical renamed", compareFieldsMeta.getIdenticalTargetStepname() );
        assertEquals( "changed renamed", compareFieldsMeta.getChangedTargetStepname() );
        assertEquals( "added renamed", compareFieldsMeta.getAddedTargetStepname() );
        assertEquals( "removed renamed", compareFieldsMeta.getRemovedTargetStepname() );
    }
}