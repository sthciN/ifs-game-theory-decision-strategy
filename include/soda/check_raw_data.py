def check(scan_name, data_source='ifs_table'):
    from soda.scan import Scan

    print(f'Soda scan on {data_source}...')

    scan = Scan()
    scan.add_configuration_yaml_file("include/soda/configuration.yml")
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files("include/soda/checks")
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()

    if result != 0:
        raise ValueError('Scan failed!')

    return result