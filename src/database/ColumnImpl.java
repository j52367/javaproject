package database;

import java.util.ArrayList;
import java.util.List;

class ColumnImpl implements Column{

    String name;
    List<String> cell = new ArrayList<String>();
    @Override
    public String getHeader() {
        return name;
    }

    @Override
    public String getValue(int index) {
        return cell.get(index);
    }

    @Override
    /**
     * @param index
     * @param t Double.class, Long.class, Integer.class
     * @return cell 값을 타입 t로 반환, cell 값이 null이면 null 반환, 타입 t로 변환 불가능한 존재하는 값에 대해서는 예외 발생
     */
    public <T extends Number> T getValue(int index, Class<T> t) {
        String s = cell.get(index);
        if(s==null){
            return null;
        }
        if(t.isAssignableFrom(Integer.class)){
            Integer i = Integer.parseInt(s);
            return (T)i;
        }
        else if(t.isAssignableFrom(Long.class)){
            Long i = Long.parseLong(s);
            return (T)i;
        }
        else if(t.isAssignableFrom(Double.class)){
            Double i = Double.parseDouble(s);
            return (T)i;
        }
        return null;
    }

    @Override
    public void setValue(int index, String value) {
        cell.set(index,value);
    }

    @Override
    public void setValue(int index, int value) {
        String v = String.valueOf(value);
        cell.set(index,v);
    }

    @Override
    public int count() {
        return cell.size();
    }

    @Override
    public void show() {
        String [] s = cell.toArray(new String[cell.size()]);
        int maxSize=name.length();
        for(int i=0;i<s.length;i++){
            if(s[i].length()>maxSize){
                maxSize = s[i].length();
            }
        }
        System.out.printf("%"+maxSize+"s |",name);
        for(int i=0;i<s.length;i++){
            System.out.printf("%"+maxSize+"s |",s[i]);
        }
    }

    @Override
    public boolean isNumericColumn() {
        String[] arr = cell.toArray(new String[cell.size()]);
        int ss=0;
        for(int j=0;j<arr.length;j++) {
            if (arr[j]!=null&&!arr[j].matches("[+-]?\\d*(\\.\\d+)?")) {
                ss++;
            }
        }
        return ss==0?true:false;
    }

    @Override
    public long getNullCount() {
        String [] s = cell.toArray(new String[cell.size()]);
        int nullCnt=0;
        for(int i=0;i<s.length;i++){
            if(s[i]==null){
                nullCnt++;
            }
        }
        return nullCnt;
    }
}
