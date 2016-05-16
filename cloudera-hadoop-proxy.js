var httpProxy = require("http-proxy");
var http = require("http");
var url = require("url");
var net = require('net');
var execSync = require('sync-exec');

// map of [host][port] -> tunneled port
var tunnels = {}

// Timeout for sync-exec.
var timeout = 10000;

// Start proxy server at this port, and continue
// tunnels from the port number onwards.
var startPort = 10000

// Name of the cluster.
// You will connect to Cloudera Manager as
// http://<cluster>:<port> or https://<cluster>:<port>
var cluster;

var ssh_userid;

// Command to login to the machine where Cloudera Manager is running
// without asking for password.
var ssh = 'ssh -o TCPKeepAlive=no -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o Compression=no -o BatchMode=no'
// ssh using a config file.
// var ssh = 'ssh -F /my/config/file/cluster.config'

// Name of the machines within the cluster
var hostmap = {}

// Host name of machine running Cloudera Manager.
hostmap[cluster] = "0.0.0.0" // MODIFY

// Command to kill all tunnels when you press Ctrl-C
function get_kill_command() {
    return "kill -9 `ps -eaf | grep 'ssh ' | grep '" + cluster + " -L' | awk '{print $2}'`";
}

function usage() {
    console.log("Usage: [-timeout <timeout for exec>] [-ssh <ssh command>] [-userid <userid to ssh>] [-hostmap hostname_or_ip:map] -proxy <port number to start proxy on> -gateway <hostname/IP of gateway machine>");
    process.exit();
}

function parse_argv() {
    for (var i = 2; i < process.argv.length; i++) {
        if (process.argv[i] == "-timeout") {
            timeout = parseInt(process.argv[++i]);
        } else if (process.argv[i] == "-ssh") {
            ssh = process.argv[++i];
        } else if (process.argv[i] == "-proxy") {
            startPort = parseInt(process.argv[++i]);
        } else if (process.argv[i] == "-gateway") {
            cluster = process.argv[++i];
        } else if (process.argv[i] == "-userid") {
            ssh_userid = process.argv[++i];
        } else if (process.argv[i] == "-hostmap") {
            map = process.argv[++i].split(':');
            hostmap[map[0]] = map[1];
        } else {
            usage();
        }
    }

    if (!cluster)
        usage();
}

function get_ssh_command() {
    var ssh_command = ssh + ' -Nf ';

    if (ssh_userid)
        ssh_command = ssh_command + ssh_userid + '@';

    return ssh_command + cluster;
}

function get_tunnel(subhost, targetport, callback) {
    var targetHost = hostmap[subhost]
    if (typeof tunnels[targetHost] === 'undefined')
        tunnels[targetHost] = {}

    var tunnel = tunnels[targetHost][targetport]

    if (typeof tunnel === 'undefined') {
        tunnel = ++startPort
        var sshCommand = get_ssh_command() + ' -L ' + tunnel + ':' + targetHost + ":" + targetport;
        // var sshCommand = ssh + ' ec2-user@' + cluster + ' -L ' + tunnel + ':' + targetHost + ":" + targetport + ' -Nf'
        exec(sshCommand, function (error) {
            tunnels[targetHost][targetport] = tunnel
            callback(tunnel)
        })
    } else {
        callback(tunnel)
    }
}

// So the program does not close instantly.
process.stdin.resume();

parse_argv();

// Exit handler, close all tunnels.
function exitHandler (options, err) {
    exec(get_kill_command(), function (error) {
        if (options.cleanup) console.log('cleanup');
        if (err) console.log(err.stack);
        if (options.exit) process.exit();
    });
}

// Catches Ctrl-C event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

function exec (command, callback) {
    console.log("Executing: " + command)
    var log = execSync(command, timeout);
    // console.log("Done executing, log is " + log)
    callback (false)
}

var regex_hostport = /^([^:]+)(:([0-9]+))?$/;

var getHostPortFromString = function (hostString, defaultPort) {
    var host = hostString;
    var port = defaultPort;

    var result = regex_hostport.exec(hostString);
    if (result != null) {
        host = result[1];
        if (result[2] != null) {
            port = result[3];
        }
    }

    return ( [host, port] );
};

function proxy_http(req, res, target) {
  var proxy = httpProxy.createProxyServer({});
  proxy.on("error", function (err, req, res) {
    console.log("proxy error", err);
    res.end();
  });

  proxy.web(req, res, {target: target});
}

var server = http.createServer(function (req, res) {
  var urlObj = url.parse(req.url);

  // All access to AWS hosts goes though tunnel.
  if (urlObj.hostname.startsWith("ec2") || urlObj.hostname.startsWith("ip-"))
      hostmap[urlObj.hostname] = urlObj.hostname

  if (urlObj.hostname in hostmap) {
      get_tunnel(urlObj.hostname, urlObj.port, function (port) {
          console.log("Proxying HTTP request for: " + req.url + " to localhost " + port);
          var target = urlObj.protocol + "//localhost:" + port;
          proxy_http(req, res, target);
      })
  } else {
      var target = urlObj.protocol + "//" + urlObj.hostname + ":" + urlObj.port;
      proxy_http(req, res, target);
  }
}).listen(startPort);  //this is the port your clients will connect to

function proxy_https(req, host, port, socket, bodyhead) {
    var proxySocket = new net.Socket();
    proxySocket.connect(port, host, function () {
        proxySocket.write(bodyhead);
        socket.write("HTTP/" + req.httpVersion + " 200 Connection established\r\n\r\n");
    });

    proxySocket.on('data', function (chunk) {
        socket.write(chunk);
    });

    proxySocket.on('end', function () {
        socket.end();
    });

    proxySocket.on('error', function () {
        socket.write("HTTP/" + req.httpVersion + " 500 Connection error\r\n\r\n");
        socket.end();
    });

    socket.on('data', function (chunk) {
        proxySocket.write(chunk);
    });

    socket.on('end', function () {
        proxySocket.end();
    });

    socket.on('error', function () {
        proxySocket.end();
    });
}

server.addListener('connect', function (req, socket, bodyhead) {
    var hostPort = getHostPortFromString(req.url, 443);
    var hostDomain = hostPort[0];
    var port = parseInt(hostPort[1]);

    // All access to AWS hosts goes though tunnel.
    if (hostDomain.startsWith("ec2") || hostDomain.startsWith("ip-"))
        hostmap[hostDomain] = hostDomain

    if (hostDomain in hostmap) {
        get_tunnel(hostDomain, port, function (port) {
            console.log("Proxying HTTPS request for: " + req.url + " to localhost " + port);
            proxy_https(req, "localhost", port, socket, bodyhead);
        })
    } else {
        proxy_https(req, hostDomain, port, socket, bodyhead);
    }
});

