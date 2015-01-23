package coprocessor.observer;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionObserverSample extends BaseRegionObserver {

  private Logger LOGGER = LoggerFactory.getLogger(RegionObserverSample.class);

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
    LOGGER.info("Coprocessor start. Region={}", env.getRegion().getRegionNameAsString());
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;
    LOGGER.info("Coprocessor stop. Region={}", env.getRegion().getRegionNameAsString());
  }

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
    LOGGER.info("preGetOp: Region=" + e.getEnvironment().getRegion().getRegionNameAsString() + ", Get=" + get);
  }
  
  @Override
  public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
    LOGGER.info("postGetOp: Region=" + e.getEnvironment().getRegion().getRegionNameAsString() + ", Get=" + get);
  }
}
