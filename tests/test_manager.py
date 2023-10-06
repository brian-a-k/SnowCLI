from snow.config.manager import ConfigManager, SnowConfig, SnowProfile
from .utils import get_resource_path


class TestConfigManager:
    _test_config_path = get_resource_path()

    def test__load(self):
        test_manager = ConfigManager(profile_name='test', base_path=self._test_config_path)
        assert isinstance(test_manager._config, SnowConfig)
        assert all([profile in test_manager._config.profiles for profile in ('test', 'default')])

    def test_write(self):
        test_manager_update = ConfigManager(profile_name='test', base_path=self._test_config_path)

        _test_profile = {
            'user': 'test_user',
            'account': 'test_account',
            'warehouse': 'test_warehouse',
            'role': 'test_role',
            'database': 'test_db',
            'schema': 'test_schema',
            'private_key_secret': 'test_secret',
            'passphrase_secret': 'test_pw'
        }

        test_manager_update.write(**_test_profile)
        assert test_manager_update._profile_name in test_manager_update._config.profiles

        test_manager_new = ConfigManager(profile_name='new', base_path=self._test_config_path)
        test_manager_new.write(**_test_profile)
        assert test_manager_new._profile_name in test_manager_new._config.profiles

    def test_read(self):
        test_manager = ConfigManager(profile_name='test', base_path=self._test_config_path)
        test_default = ConfigManager(profile_name='default', base_path=self._test_config_path)
        assert isinstance(test_manager.read(), SnowProfile)
        assert isinstance(test_default.read(), SnowProfile)
