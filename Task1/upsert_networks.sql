INSERT INTO anton_mishin_networks
    (id, name, location, href, company, gbfs_href, system, source,
     ebikes, license, scooters, instances)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name, location = EXCLUDED.location,
    href = EXCLUDED.href, company = EXCLUDED.company,
    gbfs_href = EXCLUDED.gbfs_href, system = EXCLUDED.system,
    source = EXCLUDED.source, ebikes = EXCLUDED.ebikes,
    license = EXCLUDED.license, scooters = EXCLUDED.scooters,
    instances = EXCLUDED.instances;
