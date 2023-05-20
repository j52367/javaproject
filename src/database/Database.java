package database;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class Database {
    // 테이블명이 같으면 같은 테이블로 간주된다.
    private static final Set<Table> tables = new HashSet<>();

    // 테이블 이름 목록을 출력한다.
    public static void showTables() {
        System.out.println();
        Iterator<Table> i = tables.iterator();
        while(i.hasNext()){
            System.out.println(i.next().getName());
        }
    }

    /**
     * 파일로부터 테이블을 생성하고 table에 추가한다.
     *
     * @param csv 확장자는 csv로 가정한다.
     *            파일명이 테이블명이 된다.
     *            csv 파일의 1행은 컬럼명으로 사용한다.
     *            csv 파일의 컬럼명은 중복되지 않는다고 가정한다.
     *            컬럼의 데이터 타입은 int 아니면 String으로 판정한다.
     *            String 타입의 데이터는 ("), ('), (,)는 포함하지 않는 것으로 가정한다.
     */
    public static void createTable(File csv) throws FileNotFoundException {
        Scanner s = new Scanner(csv);
        String line = s.nextLine();
        String[] a = line.split(",");
        ColumnImpl [] c= new ColumnImpl[a.length];
        //columns 이름 저장
        for(int i=0;i<a.length;i++){
            c[i]= new ColumnImpl();
            c[i].name=a[i];
        }
        //각 열을 columns에 저장
        while(s.hasNextLine()){
            line=s.nextLine();
            String[] b = line.split(",");
            for(int i=0;i<a.length;i++){
                if(i<b.length) {
                    c[i].cell.add(b[i]);
                }
                else{
                    c[i].cell.add(null);
                }
            }
        }
        //각 columns을 table에 저장
        TableImpl t =new TableImpl();
        String fFullname = csv.getName();
        String [] fName = fFullname.split("[.]");
        t.name = fName[0];
        for(int i=0;i<a.length;i++){
            t.tablecol.add(c[i]);
        }
        tables.add(t);
    }

    // tableName과 테이블명이 같은 테이블을 리턴한다. 없으면 null 리턴.
    public static Table getTable(String tableName) {
        Iterator<Table> i = tables.iterator();
        while(i.hasNext()){
            Table tmp = i.next();
            if(tmp.getName().equals(tableName)){
                return tmp;
            }
        };
        return null;
    }

    /**
     * @return 정렬된 새로운 Table 객체를 반환한다. 즉, 첫 번째 매개변수 Table은 변경되지 않는다.
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생시켜도 됨.
     */
    public static Table sort(Table table, int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        int i = table.getColumn(0).count();
        Table t = table.head(i);
        return t.sort(byIndexOfColumn,isAscending,isNullFirst);
    }
}
