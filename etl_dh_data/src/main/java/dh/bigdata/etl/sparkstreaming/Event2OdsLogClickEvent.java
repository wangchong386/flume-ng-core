package dh.bigdata.etl.sparkstreaming;
 
import dh.bigdata.etl.util.PigConv;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogClickEvent implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_clickevent中的行
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Row call(DHEvent event) throws Exception {

        return RowFactory.create(event.getName(),
                event.getId(),
                event.getVid(),
                (event.getTags().get("usrid") != null && event.getTags().get("usrid") != "") ? event.getTags().get("usrid").toString() : event.getUsrid(),
                event.getSid(),
                event.getVt(),
                event.getIp(),
                event.getUa(),
                event.getF(),
                event.getD(),
                event.getRefurl(),
                event.getRefpvid(),
                event.getPvid(),
                event.getUlevel(),
                event.getAid(),
                event.getU(),
                event.getCou(),
                PigConv.getSite(event),
                PigConv.getLang(event),
                (event.getTags().get("pt") == null) ? "" : event.getTags().get("pt").toString(),
                (event.getTags().get("loc") == null) ? "" : event.getTags().get("loc").toString(),
                (event.getTags().get("pos") == null) ? "" : event.getTags().get("pos").toString(),
                (event.getTags().get("attach") == null) ? "" : event.getTags().get("attach").toString(),
                (event.getTags().get("deviceid") == null) ? "" : event.getTags().get("deviceid").toString(),
                (event.getTags().get("activityid") == null) ? "" : event.getTags().get("activityid").toString(),
                PigConv.getItemcode(event),
                (event.getTags().get("lastvisittime") == null) ? "" : event.getTags().get("lastvisittime").toString(),
                (event.getTags().get("pvn") == null) ? "" : event.getTags().get("pvn").toString(),
                (event.getTags().get("vnum") == null) ? "" : event.getTags().get("vnum").toString(),
                (event.getTags().get("pagedur") == null) ? "" : event.getTags().get("pagedur").toString(),
                (event.getTags().get("session") == null) ? "" : event.getTags().get("session").toString(),
                event.getTags().get("currentDate").toString(),
                event.getName());
    }

}

