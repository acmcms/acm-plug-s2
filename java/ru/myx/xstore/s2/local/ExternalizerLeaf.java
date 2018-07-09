/*
 * Created on 12.09.2005
 */
package ru.myx.xstore.s2.local;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

import ru.myx.ae3.Engine;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseMessage;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferBuffer;
import ru.myx.ae3.binary.TransferCollector;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.common.Value;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.transform.SerializationRequest;
import ru.myx.ae3.transform.Transform;
import ru.myx.ae3.xml.Xml;

final class ExternalizerLeaf {
	
	private static final int LAST_VERSION_NUMBER = 3;

	private final ServerLocal server;

	private final ExternalHandler parent;

	final Object issuer;

	private final File folder;

	ExternalizerLeaf(final ServerLocal server, final ExternalHandler parent, final Object issuer, final File folder) {
		
		this.server = server;
		this.folder = folder;
		this.issuer = issuer;
		this.parent = parent;
	}

	final void check() throws Throwable {
		
		final File[] extras = this.folder.listFiles();
		if (extras != null) {
			for (int i = extras.length - 1; i >= 0; --i) {
				final File extra = extras[i];
				if (extra.isDirectory()) {
					this.clean(extra);
					continue;
				}
				if (!this.parent.hasExternal(null, extra.getName())) {
					extra.delete();
				}
			}
		}
	}

	private final void clean(final File folder) {
		
		final File[] children = folder.listFiles();
		if (children != null && children.length > 0) {
			for (int i = children.length - 1; i >= 0; --i) {
				final File child = children[i];
				if (child.isDirectory()) {
					this.clean(child);
				} else {
					child.delete();
				}
			}
		}
		folder.delete();
	}

	final External getExternal(final Object attachment, final String identifier) {
		
		final File serialized = new File(this.folder, identifier);
		try {
			if (serialized.exists()) {
				if (serialized.isDirectory()) {
					this.clean(serialized);
				} else {
					final int number;
					try (final RandomAccessFile raf = new RandomAccessFile(serialized, "r")) {
						number = raf.readInt();
					}
					if (number > ExternalizerLeaf.LAST_VERSION_NUMBER) {
						throw new RuntimeException("Version number (" + number + ") is greater than supported!");
					}
					if (number < ExternalizerLeaf.LAST_VERSION_NUMBER) {
						this.clean(serialized);
						return null;
					}
					return ExternalizerVersion3.materialize(this.server, identifier, this.issuer, serialized);
				}
			}
			synchronized (this) {
				if (serialized.isFile()) {
					return this.getExternal(attachment, identifier);
				}
				final Object extra = this.parent.getExternal(attachment, identifier);
				if (extra == null) {
					return null;
				}
				if (extra instanceof Value<?>) {
					final Object object = ((Value<?>) extra).baseValue();
					if (object == null) {
						return null;
					}
					this.putExternal(identifier, object);
				} else {
					this.putExternal(identifier, extra);
				}
			}
			return this.getExternal(attachment, identifier);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	final Object getIssuer() {
		
		return this.issuer;
	}

	final boolean hasExternal(final String identifier) throws Exception {
		
		final File target = new File(this.folder, identifier);
		if (target.isFile()) {
			return true;
		}
		return false;
	}

	private final void putExternal(final String identifier, final Object object) throws Exception {
		
		if (object.getClass() == String.class) {
			this.putExternal(identifier, "text/plain", Transfer.wrapCopier(((String) object).getBytes(Engine.CHARSET_UTF8)));
			return;
		}
		if (object instanceof byte[]) {
			this.putExternal(identifier, "ae2/bytes", Transfer.wrapCopier((byte[]) object));
			return;
		}
		if (object instanceof TransferBuffer) {
			this.putExternal(identifier, "ae2/binary", ((TransferBuffer) object).toBinary());
			return;
		}
		if (object instanceof TransferCopier) {
			this.putExternal(identifier, "ae2/binary", (TransferCopier) object);
			return;
		}
		if (object instanceof Map<?, ?>) {
			final Map<String, Object> map = Convert.Any.toAny(object);
			final String xml = Xml.toXmlString("map", Base.fromMap(map), false);
			this.putExternal(identifier, "text/xml", Transfer.wrapCopier(xml.getBytes(Engine.CHARSET_UTF8)));
			return;
		}
		if (object instanceof BaseMessage) {
			final StringBuilder contentTypeBuffer = new StringBuilder();
			final TransferCollector collector = Transfer.createCollector();
			if (!Transform.serialize(new SerializationRequest() {
				
				@Override
				public final String[] getAcceptTypes() {
					
					return null;
				}

				@Override
				public final Object getObject() {
					
					return object;
				}

				@Override
				public final Class<?> getObjectClass() {
					
					return object.getClass();
				}

				@Override
				public final TransferCollector setResultType(final String contentType) throws java.io.IOException {
					
					contentTypeBuffer.append(contentType);
					return collector;
				}
			})) {
				throw new RuntimeException("Cannot convert an object, class=" + object.getClass().getName() + "!");
			}
			final TransferCopier copier = collector.toCloneFactory();
			this.putExternal(identifier, contentTypeBuffer.toString(), copier);
			return;
		}
		throw new IllegalArgumentException("Cannot serialize, class=" + object.getClass());
	}

	final void putExternal(final String identifier, final String type, final TransferCopier copier) throws Exception {
		
		this.folder.mkdirs();
		final File target = new File(this.folder, identifier);
		synchronized (this) {
			try {
				if (target.isDirectory()) {
					this.clean(target);
				}
				ExternalizerVersion3.serialize(target, Engine.fastTime(), type, copier);
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
