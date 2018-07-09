import ru.myx.ae1.AcmPluginFactory;
import ru.myx.ae1.PluginInstance;
import ru.myx.ae3.base.BaseObject;
import ru.myx.xstore.s2.StorageLevel2;

/*
 * Created on 07.10.2003
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
final class PluginLevel2Factory implements AcmPluginFactory {
	
	private static final String[] VARIETY = {
			"ACMMOD:STORAGE2"
	};

	@Override
	public final PluginInstance produce(final String variant, final BaseObject attributes, final Object source) {
		
		return new StorageLevel2();
	}

	@Override
	public final String[] variety() {
		
		return PluginLevel2Factory.VARIETY;
	}
}
