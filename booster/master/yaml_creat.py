def yamls_creat():
    import yaml

    raw = False

    registry_name = ["", "fengyicheng/"]
    version_list = {'python': '3.9.3', 'golang': '1.16.2', 'openjdk': '11.0.11-9-jdk', 'alpine':'3.13.4', 'ubuntu':'focal-20210401', 'memcached':'1.6.8', 'nginx':'1.19.10', 'httpd':'2.4.43', 'mysql':'8.0.23', 'mariadb':'10.5.8', 'redis':'6.2.1', 'mongo':'4.0.23', 'postgres':'13.1', 'rabbitmq':'3.8.13', 'registry':'2.7.0', 'wordpress':'php7.3-fpm', 'ghost':'3.42.5-alpine', 'node':'16-alpine3.11', 'flink':'1.12.3-scala_2.11-java8', 'cassandra':'3.11.9', 'eclipse-mosquitto':'2.0.9-openssl'}
    image_list = [img for img in version_list]

    image_list = ["rabbitmq", "wordpress", "node", "ghost", "openjdk",  "flink", "redis", "mysql", "ubuntu",
                   "python", "eclipse-mosquitto", "golang", "alpine",  "postgres", "cassandra", "mariadb",
                   "memcached", "httpd", "registry"]

    for app_name in version_list:
        with open('./yamls/req1.yaml', 'r') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
        data['metadata']['name'] = app_name
        # data['spec']['selector']['matchLabels']['app'] = app_name
        data['spec']['template']['spec']['containers'][0]['name'] = app_name #+ "-" + version_list[app_name]
        data['spec']['template']['spec']['containers'][0]['image'] = registry_name[0] + app_name + ":" + version_list[
            app_name] if raw else registry_name[1] + app_name + ":" + version_list[app_name]
        new_yaml_name = app_name + ":" + version_list[app_name] + ".yaml"
        dir_head = './yamls/' + "raw/" if raw else './yamls/' + "mine/"
        with open(dir_head + new_yaml_name, 'w') as f:
            yaml.dump(data, f)


if __name__ == '__main__':
    yamls_creat()