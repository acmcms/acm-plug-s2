import ru.myx.ae3.produce.Produce;

/*
 * Created on 07.10.2003
 * 
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window>Preferences>Java>Code Generation>Code and Comments
 */
public final class Main {
	/**
	 * @param args
	 */
	public static final void main(final String[] args) {
		System.out.println( "RU.MYX.AE1PLUG.STORAGE(2): plugin: ACM [StorageL2] is being initialized..." );
		Produce.registerFactory( new CommandClearCacheAllFactory() );
		Produce.registerFactory( new CommandClearCacheTypesFactory() );
		Produce.registerFactory( new PluginLevel2Factory() );
	}
}
