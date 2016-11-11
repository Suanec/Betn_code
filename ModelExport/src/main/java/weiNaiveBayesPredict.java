import java.lang.*;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

public class weiNaiveBayesPredict {
  static Double[][] theta = null;
  static Double[] labels = null;
  static Double[] pi = null;
  static String modelType = null;
  public static void InitParam( String[] _modelData ) {
    int idx = 0;
    modelType = _modelData[idx++].split(" ")[1];
//    Integer.parseInt(s)
    int classCount = new Integer(_modelData[idx++].split(" ")[1]).intValue();
    labels = new Double[classCount];
    String[] splits = _modelData[idx++].split(" ");
    for(int i = 0; i < labels.length; i++){
      labels[i] = new Double(splits[1+i]).doubleValue();
    }
    pi = new Double[classCount];
    splits = _modelData[idx++].split(" ");
    for(int i = 0; i < classCount; i++){
      pi[i] = new Double(splits[1+i]).doubleValue();
    }
    int featuresCount = new Integer(_modelData[idx++].split(" ")[1]).intValue();
    theta = new Double[featuresCount][];
    for(int i = 0; i < featuresCount; i++) theta[i] = new Double[classCount];
    idx++;
    for(int i = 0; i < featuresCount; i++){
      splits = _modelData[idx++].split(" ");
      for(int j = 0; j < theta[i].length; j++) theta[i][j] = Double.parseDouble(splits[j]);
    }
  }
  public static Integer findMax( Double[] _arr ) {
    if(_arr == null) return -1;
    if(_arr.length <= 0) return -2;
    int idx = 0;
    Double maxValue = _arr[0];
    for(int i = idx; i < _arr.length; i++){
      if(_arr[i]>maxValue) {
        idx = i;
        maxValue = _arr[i];
      }
    }
    return idx;
  }
  public static Double predict( String _data ) {
    if(theta == null) return -1d;
    String[] splits = _data.split(" ");
    Double[] rst = new Double[theta[0].length];
    for(int i = 0; i < rst.length; i++) rst[i] = pi[i];

    /// theta^T * x + pi => Vector(rst) 
    for(int i = 1; i < splits.length; i++){
      String[] values = splits[i].split(":");
      int idx = Integer.parseInt(values[0]);
      Double value = Double.parseDouble(values[1]);
      for(int j = 0; j < rst.length; j++) {
//        System.out.println(j);
//        System.out.println(rst[j]);
//        System.out.println(idx);
//        System.out.println(value);
//        System.out.println(theta[idx-1]);
        rst[j] += (theta[idx-1][j] * value);
      }
    }
    return labels[findMax(rst)];
  }
  public static Double[] predict( String[] _data ) {
    Double[] rst = new Double[_data.length];
    for( int i = 0; i < _data.length; i++ ) rst[i] = predict(_data[i]);
    return rst;
  }
  public static Double validACC( String exC ) {
    String[] data = readFileByLines(exC).toArray(new String[0]);
    Double[] rst = predict(data);
    Integer sum = 0;
    for(int i = 0; i < rst.length; i++) if(rst[i] == Double.parseDouble(data[i].split(" ")[0])) sum++;
    return (sum * 1d)/rst.length;
  }
  public static List<String> readFileByLines(String fileName) {
    File file = new File(fileName);  
    BufferedReader reader = null;  
    List<String> list = new LinkedList<String>();
//    List<String> theList = new LinkedList<String>();
    try {  
      reader = new BufferedReader(new FileReader(file));  
      String tempString = "";
      while (tempString != null) {  
        tempString = reader.readLine();
        if(tempString != null) list.add(tempString.toString());
      }  
      reader.close();  
    } catch (IOException e) {  
      e.printStackTrace();  
    } finally {  
      if (reader != null) {  
        try {  
          reader.close();  
        } catch (IOException e1) {  
        }  
      }  
    }
//    String[] fruits = theList.toArray(new String[0]);
    return list;
  }
  
  /** 
   * @param args 
   */  
  public static void main(String[] args) {  
    // TODO Auto-generated method stub
    String path = "D:\\Docs\\Works_And_Jobs\\Sina\\Betn_code\\NaiveBayesModelExport";
    String saveFile = path + "\\rst\\save.model";
    String exC = path + "\\data" + "\\extract_combined_data.rst";
    List<String> modelData = readFileByLines(saveFile);
    System.out.println(modelData.get(0));
    InitParam(modelData.toArray(new String[0]));
    Double acc = validACC(exC);
    System.out.println(acc);
  }  
}  