/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.generate;

import com.google.common.base.Preconditions;
import org.apache.mahout.math.list.DoubleArrayList;

import java.util.Random;

/**
 * Sample from an infinite dimensional multinomial whose parameters are sampled from a Pittman-Yor
 * process.  If discount = 0, this reduces to the normal Chinese Restaurant process.  For discount
 * near 1, you should get a power law type of distribution.
 *
 *
 */
public class ChineseRestaurant implements Sampler<Integer> {
  private double alpha;
  private double weight = 0;
  private double discount = 0;
  private DoubleArrayList weights = new DoubleArrayList();

  private Random rand = new Random();

  public ChineseRestaurant(double alpha, double discount) {
    Preconditions.checkArgument(alpha > 0);
    Preconditions.checkArgument(discount >= 0 && discount <= 1);
    this.alpha = alpha;
    this.discount = discount;
  }

  public Integer sample() {
    double u = rand.nextDouble() * (alpha + weight);
    for (int j = 0; j < weights.size(); j++) {
      // select existing options with probability (w_j - d) / (alpha + w)
      if (u < weights.get(j) - discount) {
        weights.set(j, weights.get(j) + 1);
        weight++;
        return j;
      } else {
        u -= weights.get(j) - discount;
      }
    }

    // if no existing item selected, pick new item with probability (alpha - d*t)/ (alpha + w)
    // where t is number of pre-existing cases
    weights.add(1);
    weight++;
    return weights.size() - 1;
  }

  public int size() {
    return weights.size();
  }

  public int count() {
    return (int) weight;
  }

  public int count(int j) {
    Preconditions.checkArgument(j >= 0);

    if (j < weights.size()) {
      return (int) weights.get(j);
    } else {
      return 0;
    }
  }
}
