package com.zdp.dao;

import lombok.Data;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class ZDPModuleHistory implements Component {

    // this variable is exactly matches  with project table column
    private long id; //
    private long module_id;
    private int version;
    private String oozie_id;
    private long project_run_id;
    private String output_blob;
    private Timestamp start_time;
    private Timestamp end_time;
    private String created_by;
    private String status;
    private String details;


    public ZDPModuleHistory() {

    }

    public ZDPModuleHistory(long id, long module_id, int version,
                            long project_run_id, String output_blob, Timestamp start_time,
                            Timestamp end_time, String created_by, String status, String details, String oozie_id) {
        this.id = id;
        this.module_id = module_id;
        this.version = version;
        this.project_run_id = project_run_id;
        this.output_blob = output_blob;
        this.start_time = start_time;
        this.end_time = end_time;
        this.created_by = created_by;
        this.status = status;
        this.details = details;
        this.oozie_id = oozie_id;
    }

    /*
     * return the column and their datatype
     */
    public Map<String, String> getColumnNameAndType() {

        Map<String, String> columnAndType = new LinkedHashMap<String, String>();
        Class<?> objClass = this.getClass();
        Field[] fields = objClass.getDeclaredFields();
        for (Field f : fields) {
            if (!(f.getName().equals("start_time") || f.getName().equals("end_time")))
                columnAndType.put(f.getName(), f.getType().getSimpleName());
        }

        return columnAndType;
    }

    public Map<String, String> getColumnNameAndValue() {

        Map<String, String> columnNameAndValue = new LinkedHashMap<String, String>();
        //Map<String, String> columnAndType = new LinkedHashMap<String, String>();
        Class<?> objClass = this.getClass();
        Field[] fields = objClass.getDeclaredFields();
        for (Field f : fields) {
            try {
                System.out.println(f.getName() + " value :" + f.get(this));
                String key = f.getName();
                Object val = f.get(this);
                if (!(f.getName().equals("start_time") || f.getName().equals("end_time"))) {
                    if (val != null) {
                        columnNameAndValue.put(f.getName(), val.toString());
                    } else {
                        columnNameAndValue.put(key, "");
                    }

                }
            } catch (IllegalArgumentException |IllegalAccessException e) {
                e.printStackTrace();
            }

        }

        return columnNameAndValue;
    }

}
