<?php
// pfSense node_exporter 패키지 설정을 config.xml에 맞춘다. 인자: <인터페이스> <포트>
// 웹 화면(Services → Prometheus node_exporter)에서 Enable · Interface · Listen port를 저장하는 것과 같은 일을 한다.
// 이미 같은 값이고 서비스가 떠 있으면 아무것도 하지 않고 "unchanged"를 낸다(Ansible changed 판정용)
require_once("config.inc");
require_once("service-utils.inc");
require_once("/usr/local/pkg/node_exporter.inc");

$path = 'installedpackages/nodeexporter/config/0';
$cur = config_get_path($path, []);
if (!is_array($cur)) {
	$cur = [];
}
$new = array_merge($cur, ["enable" => "on", "listen_iface" => $argv[1], "listen_port" => $argv[2]]);

if ($new == $cur && is_service_running('node_exporter')) {
	echo "unchanged\n";
	exit(0);
}
if ($new != $cur) {
	config_set_path($path, $new);
	write_config("node_exporter: Monitoring 레포 Ansible(pfsense_node_exporter)로 설정");
}
// rc 파일을 쓰고 서비스를 (다시) 띄운다
node_exporter_sync_config();
echo "changed\n";
