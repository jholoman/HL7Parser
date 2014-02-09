package org.choa.hadoop.HL7Parser;

/**
 * Created with IntelliJ IDEA.
 * User: jholoman
 * Date: 2/9/14
 * Time: 11:26 AM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


import java.io.IOException;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.message.ORU_R01;
import ca.uhn.hl7v2.model.v23.segment.MSH;
import ca.uhn.hl7v2.model.v23.segment.PID;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import ca.uhn.hl7v2.model.v23.segment.OBX;
import ca.uhn.hl7v2.parser.EncodingNotSupportedException;
import ca.uhn.hl7v2.parser.Parser;


public class HL7Mapper extends
        Mapper<LongWritable, Text, NullWritable, Text> {
        private static final Logger logger = Logger.getLogger("map");


    @Override
    public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException{
        System.out.println("Mapper is about to process: " + value.toString());

        String v = value.toString();//.replaceAll("\n", "\r");

        Text val = new Text ();
        String s = "|";

        HapiContext hcontext = new DefaultHapiContext();
        Parser p = hcontext.getGenericParser();
        Message hapiMsg;

        try {
            // The parse method performs the actual parsing
            hapiMsg = p.parse(v);
        } catch (EncodingNotSupportedException e) {
            e.printStackTrace();
            return;
        } catch (HL7Exception e) {
            e.printStackTrace();
            return;
        }

        StringBuffer finalString = new StringBuffer();
        StringBuffer outputDetailRepeat = new StringBuffer();
        StringBuffer outputDetailOBX = new StringBuffer();

        //ORU is the message type from monitors
        ORU_R01 oruMsg = (ORU_R01)hapiMsg;

        //MSH segment
        MSH msh = oruMsg.getMSH();

        outputDetailRepeat.append(msh.getMessageType().getMessageType().getValue()).append(s);
        outputDetailRepeat.append(msh.getMessageType().getTriggerEvent().getValue()).append(s);
        outputDetailRepeat.append(msh.getMessageControlID().getValue()).append(s);
        outputDetailRepeat.append(msh.getProcessingID().getComponents()[0].toString()).append(s);
        outputDetailRepeat.append(msh.getVersionID().getValue()).append(s);
        outputDetailRepeat.append(msh.getCharacterSet().getValue()).append(s);
        //logger.info(outputDetailRepeat.toString() + ": after MSH");

        //PID segment
        PID pid = oruMsg.getRESPONSE().getPATIENT().getPID();
        //logger.info(pid.toString() + ": PID");
        outputDetailRepeat.append(pid.getPid3_PatientIDInternalID(0).getCx1_ID().getValue()).append(s);
        //logger.info(outputDetailRepeat.toString() + ": after PID line 1");
        outputDetailRepeat.append(pid.getPid5_PatientName(0).getXpn1_FamilyName().getValue()).append(s);
        //logger.info(outputDetailRepeat.toString() + ": after PID line 2");
        outputDetailRepeat.append(pid.getPid5_PatientName(0).getXpn2_GivenName().getValue()).append(s);
        outputDetailRepeat.append(pid.getDateOfBirth().getComponents()[0].toString()).append(s);
        outputDetailRepeat.append(pid.getSex().getValue()).append(s);
        //logger.info(outputDetailRepeat.toString() + ": after PID");
        //PV1 segment
        PV1 pv1 = oruMsg.getRESPONSE().getPATIENT().getVISIT().getPV1();
        //logger.info(pv1.toString() + ": PV1");
        outputDetailRepeat.append(pv1.getPatientClass().getValue()).append(s);
        outputDetailRepeat.append(pv1.getAssignedPatientLocation().getPl1_PointOfCare().getValue()).append(s);
        outputDetailRepeat.append(pv1.getAssignedPatientLocation().getPl3_Bed().getValue()).append(s);
        //logger.info(outputDetailRepeat.toString() + ": after PVI");

        //ORU segment
        outputDetailRepeat.append(oruMsg.getRESPONSE().getORDER_OBSERVATION().getOBR().getObr7_ObservationDateTime().getTimeOfAnEvent().getValue()).append(s);
        //logger.info(outputDetailRepeat.toString() + ": after ORU");

        //OBX segment(s)
        int obxCount = oruMsg.getRESPONSE().getORDER_OBSERVATION().getOBSERVATIONReps();
        //logger.info(obxCount);
        for (int i = 0; i < obxCount; i++) {
            //logger.info("hey dude");
            OBX obx = oruMsg.getRESPONSE().getORDER_OBSERVATION().getOBSERVATION(i).getOBX();
            outputDetailOBX.append(obx.getValueType().toString()).append(s);
            outputDetailOBX.append(obx.getObservationIdentifier().getCe1_Identifier().getValue()).append(s);
            outputDetailOBX.append(obx.getObservationIdentifier().getText().getValue()).append(s);
            outputDetailOBX.append(obx.getObservationIdentifier().getCe3_NameOfCodingSystem().getValue()).append(s);
            outputDetailOBX.append(obx.getObservationSubID().getValue()).append(s);
            outputDetailOBX.append(obx.getObservationValue()[0].getData().toString()).append(s);
            outputDetailOBX.append(obx.getObx6_Units().getCe1_Identifier().getValue()).append(s);
            outputDetailOBX.append(obx.getObx6_Units().getCe2_Text().getValue()).append(s);
            outputDetailOBX.append(obx.getObx6_Units().getCe3_NameOfCodingSystem().getValue()).append(s);
            outputDetailOBX.append(obx.getObx11_ObservResultStatus().getValue()); // do not append(s).  last in the row

            // print the repeating segment and OBX segment to the file


            finalString.append(outputDetailRepeat.toString() +  outputDetailOBX.toString() + (char) 10);


            // clear the OBX segment for the next loop
            outputDetailOBX.delete(0, outputDetailRepeat.length());

        }
        // strip trailing return
        finalString.setLength(finalString.length() - 1);
        val.set(finalString.toString());
        context.write(null,
                val);

    }

}
