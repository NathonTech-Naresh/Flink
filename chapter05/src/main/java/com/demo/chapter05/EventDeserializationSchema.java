package com.demo.chapter05;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class EventDeserializationSchema implements DeserializationSchema<TemperatureEvent> {


	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public TemperatureEvent deserialize(byte[] bytes) throws IOException {
		return mapper.readValue( bytes, TemperatureEvent.class );
	}


	@Override
	public TypeInformation<TemperatureEvent> getProducedType() {
		return TypeInformation.of(new TypeHint<TemperatureEvent>(){});
	}


	public boolean isEndOfStream(TemperatureEvent temperatureEvent) {
		return false;
	}

}
