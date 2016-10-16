package com.kodcu.util.codecs;

import org.bson.codecs.configuration.CodecRegistry;

import com.mongodb.DBRefCodec;

public class CustomDBRefCodec extends DBRefCodec{

	public CustomDBRefCodec(CodecRegistry registry) {
		super(registry);
	}

}
