package com.garmin.dsii.datahub.dto

import com.linkedin.common.DatasetUrnArray
import com.linkedin.common.urn.{DataFlowUrn, DataJobUrn}


case class DataLineage(
                        inputDatasets: DatasetUrnArray,
                        outputDatasets: DatasetUrnArray,
                        dataFlow: DataFlowUrn,
                        dataJobUrn: DataJobUrn
                      )
