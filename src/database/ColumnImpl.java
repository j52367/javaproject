package database;

import java.util.ArrayList;
import java.util.List;

class ColumnImpl implements Column{

    String name;
    List<String> cell = new ArrayList<String>();
    ColumnImpl(){
    }
    @Override
    public String getHeader() {
        return name;
    }

    @Override
    public String getValue(int index) {
        return cell.get(index);
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        return null;
    }

    @Override
    public void setValue(int index, String value) {
        cell.set(index,value);
    }

    @Override
    public void setValue(int index, int value) {
//        cell.set(index,getValue(index,[뭐넣어야함]));
    }

    @Override
    public int count() {
        return cell.size();
    }

    @Override
    public void show() {

    }

    @Override
    public boolean isNumericColumn() {
        String[] arr = cell.toArray(new String[cell.size()]);
        return arr[1].matches("[+-]?\\d*(\\.\\d+)?")?true:false;
    }

    @Override
    public long getNullCount() {
        return 0;
    }
}
