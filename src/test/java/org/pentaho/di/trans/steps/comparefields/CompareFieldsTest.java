package org.pentaho.di.trans.steps.comparefields;

import org.junit.*;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class CompareFieldsTest {
    private StepMockHelper<CompareFieldsMeta, CompareFieldsData> smh;

    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

    @BeforeClass
    public static void init() throws KettleException {
        KettleEnvironment.init( false );
    }

    @Before
    public void setUp() {
        smh = new StepMockHelper<CompareFieldsMeta, CompareFieldsData>( "Compare fields",
                CompareFieldsMeta.class, CompareFieldsData.class );
        when( smh.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
                .thenReturn( smh.logChannelInterface );
        when( smh.trans.isRunning() ).thenReturn( true );
    }

    @After
    public void cleanUp() {
        smh.cleanUp();
    }

    @Test(expected = KettleException.class)
    public void testCompareNoFieldsError() throws KettleException {
        RowMeta inputRowMeta = new RowMeta();
        ValueMetaString refMeta = new ValueMetaString("ref_field");
        inputRowMeta.addValueMeta(refMeta);
        ValueMetaString cmpMeta = new ValueMetaString("cmp_field");
        inputRowMeta.addValueMeta(cmpMeta);

        RowSet inputRowSet = smh.getMockInputRowSet( new Object[][] {
                { new String( "foo" ), new Long( 10 ) },
                { new String( "foo" ), new Long( 30 ) }
        } );
        inputRowSet.setRowMeta( inputRowMeta );

        CompareFieldsMeta meta = new CompareFieldsMeta();

        CompareFields compareFields = new CompareFields( smh.stepMeta, smh.stepDataInterface, 0,
                smh.transMeta, smh.trans );
        compareFields.addRowSetToInputRowSets( inputRowSet );
        compareFields.setInputRowMeta( inputRowMeta );
        compareFields.init( smh.initStepMetaInterface, smh.initStepDataInterface );

        compareFields.processRow( meta, new CompareFieldsData() );
    }

    @Ignore("TODO: still not sure how to test output rows of various target steps")
    @Test
    public void testCompareFieldNoDiff() throws KettleException {
        RowMeta inputRowMeta = new RowMeta();
        ValueMetaString refMeta = new ValueMetaString("ref_field");
        inputRowMeta.addValueMeta(refMeta);
        ValueMetaString cmpMeta = new ValueMetaString("cmp_field");
        inputRowMeta.addValueMeta(cmpMeta);

        RowSet inputRowSet = smh.getMockInputRowSet( new Object[][] {
                { new String( "foo" ), new Long( 10 ) },
                { new String( "foo" ), new Long( 30 ) }
        } );
        inputRowSet.setRowMeta( inputRowMeta );

        StepMetaInterface identical = new DummyTransMeta();
        StepMeta identicalStepMeta = new StepMeta( "identical", identical );
        StepMetaInterface changed = new DummyTransMeta();
        StepMeta changedStepMeta = new StepMeta( "changed", changed );

        CompareFieldsMeta meta = new CompareFieldsMeta();
        //meta.setAddingFieldsList(true);
        //meta.setFieldsListFieldname("changes");
        meta.setIdenticalTargetStepMeta(identicalStepMeta);
        meta.setChangedTargetStepMeta(changedStepMeta);

        CompareField compareField = new CompareField( inputRowMeta.getFieldNames()[0], inputRowMeta.getFieldNames()[1]);
        meta.getCompareFields().add( compareField );

        CompareFields compareFields = new CompareFields( smh.stepMeta, smh.stepDataInterface, 0, smh.transMeta, smh.trans );
        compareFields.addRowSetToInputRowSets( inputRowSet );
        compareFields.setInputRowMeta( inputRowMeta );
        compareFields.init( smh.initStepMetaInterface, smh.initStepDataInterface );

        //Verify output
        compareFields.addRowListener( new RowAdapter() {
            @Override public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
                assertEquals( null, row[ 2 ] );
            }
        } );
        compareFields.processRow( meta, new CompareFieldsData() );
    }

    @Ignore("TODO: still not sure how to test output rows of various target steps")
    @Test
    public void testCompareFieldDiff() throws KettleException {
        RowMeta inputRowMeta = new RowMeta();
        ValueMetaString refMeta = new ValueMetaString("ref_field");
        inputRowMeta.addValueMeta(refMeta);
        ValueMetaString cmpMeta = new ValueMetaString("cmp_field");
        inputRowMeta.addValueMeta(cmpMeta);

        RowSet inputRowSet = smh.getMockInputRowSet( new Object[][] {
                { new String( "foo" ), new Long( 10 ) },
                { new String( "bar" ), new Long( 30 ) }
        } );
        inputRowSet.setRowMeta( inputRowMeta );

        CompareFieldsMeta meta = new CompareFieldsMeta();
        meta.setAddingFieldsList(true);
        meta.setFieldsListFieldname("changes");

        CompareField compareField = new CompareField( inputRowMeta.getFieldNames()[0], inputRowMeta.getFieldNames()[1]);
        meta.getCompareFields().add( compareField );

        CompareFields compareFields = new CompareFields( smh.stepMeta, smh.stepDataInterface, 0, smh.transMeta, smh.trans );
        compareFields.addRowSetToInputRowSets( inputRowSet );
        compareFields.setInputRowMeta( inputRowMeta );
        compareFields.init( smh.initStepMetaInterface, smh.initStepDataInterface );

        compareFields.processRow( meta, new CompareFieldsData() );
        compareFields.processRow( meta, new CompareFieldsData() );

    }
}
