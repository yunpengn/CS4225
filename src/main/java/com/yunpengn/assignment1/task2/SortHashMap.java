package com.yunpengn.assignment1.task2;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class SortHashMap {
  private List<Entry<String, Float>> list = new LinkedList<>();

  public static void main(String[] args) {
    // for test
    HashMap<String, Float> omap = new HashMap<>();
    omap.put("x", (float) 1.0);
    omap.put("y", (float) 3.0);
    omap.put("z", (float) 2.0);

    List<Entry<String, Float>> list = SortHashMap.sortHashMap(omap);
    for (Entry<String, Float> iList : list) {
      System.out.println(iList.getKey() + "\t" + iList.getValue());
    }
  }

  /**
   * inverted order
   */
  public static List<Entry<String, Float>> sortHashMap(HashMap<String, Float> map) {
    SortHashMap sorthashmap = new SortHashMap();
    sorthashmap.list.addAll(map.entrySet());
    sorthashmap.list.sort((obj1, obj2) -> Float.compare(Float.parseFloat(obj2.getValue().toString()),
        Float.parseFloat(obj1.getValue().toString())));
    return sorthashmap.list;
  }
}
