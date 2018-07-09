/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.TransferCopier;

/**
 * @author myx
 * 
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final class DataJdbc {
	private final ServerJdbc	server;
	
	private final String		objId;
	
	private final int			intDataType;
	
	private TransferCopier		intData;
	
	private BaseObject		data	= null;
	
	DataJdbc(final ServerJdbc server, final String objId, final BaseObject data) {
		this.server = server;
		this.intDataType = -1;
		this.objId = objId;
		this.data = data;
	}
	
	DataJdbc(final ServerJdbc server, final String objId, final int intDataType, final TransferCopier intData) {
		this.server = server;
		this.objId = objId;
		this.intDataType = intDataType;
		this.intData = intData;
	}
	
	final BaseObject getData() {
		if (this.data == null) {
			synchronized (this) {
				if (this.data == null) {
					try {
						this.data = MatData.dataMaterialize( this.server, this.intDataType, this.intData, null );
						this.intData = null;
					} catch (final RuntimeException e) {
						throw e;
					} catch (final Exception e) {
						throw new RuntimeException( e );
					}
				}
			}
		}
		return this.data;
	}
	
	@Override
	public String toString() {
		return "DataJdbc {objId=" + this.objId + "}";
	}
}
