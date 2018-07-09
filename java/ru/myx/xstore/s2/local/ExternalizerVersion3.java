package ru.myx.xstore.s2.local;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExtraBinary;
import ru.myx.ae3.extra.ExtraBytes;
import ru.myx.ae3.extra.ExtraSerialized;
import ru.myx.ae3.extra.ExtraTextBase;
import ru.myx.ae3.extra.ExtraXml;

final class ExternalizerVersion3 {
	static final External materialize(final ServerLocal server, final String key, final Object issuer, final File data)
			throws IOException {
		if (data.exists()) {
			try (final DataInputStream din = new DataInputStream( new FileInputStream( data ) )) {
				final int version = din.readInt();
				if (version != 3) {
					throw new RuntimeException( "Unsupported version!" );
				}
				final long date = din.readLong();
				final String type = din.readUTF();
				final int skip = din.readInt();
				final TransferCopier copier = Transfer.createCopier( data ).slice( skip, (int) data.length() - skip );
				if ("ae2/bytes".equals( type )) {
					return new ExtraBytes( issuer, key, date, copier );
				}
				if ("ae2/binary".equals( type )) {
					return new ExtraBinary( issuer, key, date, copier );
				}
				if ("text/plain".equals( type )) {
					return new ExtraTextBase( issuer, key, date, copier );
				}
				if ("text/xml".equals( type )) {
					return new ExtraXml( issuer, key, date, copier, server.getStorageExternalizer(), null );
				}
				return new ExtraSerialized( issuer, key, date, type, copier );
			}
		}
		return null;
	}
	
	static final void serialize(final File binary, final long date, final String type, final TransferCopier copier)
			throws Exception {
		final ByteArrayOutputStream binaryHeader = new ByteArrayOutputStream();
		try (final DataOutputStream dout = new DataOutputStream( binaryHeader )) {
			dout.writeInt( 3 );
			dout.writeLong( date );
			dout.writeUTF( type );
			dout.flush();
			dout.writeInt( binaryHeader.size() + 4 );
		}
		@SuppressWarnings("resource")
		final FileOutputStream out = new FileOutputStream( binary );
		try {
			out.write( binaryHeader.toByteArray() );
		} catch (final Exception e) {
			out.close();
			throw e;
		}
		Transfer.toStream( copier.nextCopy(), out, true );
	}
}
