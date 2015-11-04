var httpProxy = require("http-proxy");
var http = require("http");
var url = require("url");
var net = require('net');
var execSync = require('exec-sync');

// map of [host][port] -> tunneled port
var tunnels = {}

// Start proxy server at this port, and continue
// tunnels from the port number onwards.
var startPort = 10000 // MODIFY

// Name of the cluster.
// You will connect to Cloudera Manager as
// http://<cluster>:<port> or https://<cluster>:<port>
var cluster = 'mycluster' // MODIFY

// Command to login to the machine where Cloudera Manager is running
// without asking for password.
var ssh = 'ssh' // MODIFY
// ssh using a config file.
// var ssh = 'ssh -F /my/config/file/cluster.config'

// Name of the machines within the cluster
var hostmap = { // MODIFY
    "cdh1.cluster-internal": "cdh1",
    "cdh2.cluster-internal": "cdh2",
    "cdh3.cluster-internal": "cdh3",
}

// Host name of machine running Cloudera Manager.
hostmap[cluster] = "cm-node" // MODIFY

// Command to kill all tunnels when you press Ctrl-C
var killCommand = "kill -9 `ps -eaf | grep user | grep 'ssh ' | grep -v grep | grep '" + cluster + " -L' | awk '{print $2}'`"; // MODIFY

function get_tunnel(subhost, targetport, callback) {
    var targetHost = hostmap[subhost]
    if (typeof tunnels[targetHost] === 'undefined')
        tunnels[targetHost] = {}

    var tunnel = tunnels[targetHost][targetport]

    if (typeof tunnel === 'undefined') {
        tunnel = ++startPort
        var sshCommand = ssh + ' ' + cluster + ' -L ' + tunnel + ':' + targetHost + ":" + targetport + ' -N -f'
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

// Exit handler, close all tunnels.
function exitHandler (options, err) {
    exec(killCommand, function (error) {
        if (options.cleanup) console.log('cleanup');
        if (err) console.log(err.stack);
        if (options.exit) process.exit();
    });
}

// Catches Ctrl-C event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

function exec (command, callback) {
    console.log("Executing: " + command)
    execSync(command);
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

    if (hostDomain in hostmap) {
        get_tunnel(hostDomain, port, function (port) {
            console.log("Proxying HTTPS request for: " + req.url + " to localhost " + port);
            proxy_https(req, "localhost", port, socket, bodyhead);
        })
    } else {
        proxy_https(req, hostDomain, port, socket, bodyhead);
    }
});
