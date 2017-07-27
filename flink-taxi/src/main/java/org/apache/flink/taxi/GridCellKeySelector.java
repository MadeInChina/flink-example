package org.apache.flink.taxi;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by pnowojski on 6/14/17.
 */
class GridCellKeySelector implements KeySelector<TaxiRide, Tuple2<Integer, Boolean>> {
    @Override
    public Tuple2<Integer, Boolean> getKey(TaxiRide taxiRide) throws Exception {
        int cell;
        if (taxiRide.isStart) {
            cell = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
        } else {
            cell = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
        }
        return Tuple2.of(cell, taxiRide.isStart);

    }
}
