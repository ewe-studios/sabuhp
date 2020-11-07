package httpub

import (
	"mime"
	"path/filepath"
)

// HTTP status codes as registered with IANA.
// See: https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml
const (
	StatusContinue           = 100 // RFC 7231, 6.2.1
	StatusSwitchingProtocols = 101 // RFC 7231, 6.2.2
	StatusProcessing         = 102 // RFC 2518, 10.1
	StatusEarlyHints         = 103 // RFC 8297

	StatusOK                   = 200 // RFC 7231, 6.3.1
	StatusCreated              = 201 // RFC 7231, 6.3.2
	StatusAccepted             = 202 // RFC 7231, 6.3.3
	StatusNonAuthoritativeInfo = 203 // RFC 7231, 6.3.4
	StatusNoContent            = 204 // RFC 7231, 6.3.5
	StatusResetContent         = 205 // RFC 7231, 6.3.6
	StatusPartialContent       = 206 // RFC 7233, 4.1
	StatusMultiStatus          = 207 // RFC 4918, 11.1
	StatusAlreadyReported      = 208 // RFC 5842, 7.1
	StatusIMUsed               = 226 // RFC 3229, 10.4.1

	StatusMultipleChoices   = 300 // RFC 7231, 6.4.1
	StatusMovedPermanently  = 301 // RFC 7231, 6.4.2
	StatusFound             = 302 // RFC 7231, 6.4.3
	StatusSeeOther          = 303 // RFC 7231, 6.4.4
	StatusNotModified       = 304 // RFC 7232, 4.1
	StatusUseProxy          = 305 // RFC 7231, 6.4.5
	_                       = 306 // RFC 7231, 6.4.6 (Unused)
	StatusTemporaryRedirect = 307 // RFC 7231, 6.4.7
	StatusPermanentRedirect = 308 // RFC 7538, 3

	StatusBadRequest                   = 400 // RFC 7231, 6.5.1
	StatusUnauthorized                 = 401 // RFC 7235, 3.1
	StatusPaymentRequired              = 402 // RFC 7231, 6.5.2
	StatusForbidden                    = 403 // RFC 7231, 6.5.3
	StatusNotFound                     = 404 // RFC 7231, 6.5.4
	StatusMethodNotAllowed             = 405 // RFC 7231, 6.5.5
	StatusNotAcceptable                = 406 // RFC 7231, 6.5.6
	StatusProxyAuthRequired            = 407 // RFC 7235, 3.2
	StatusRequestTimeout               = 408 // RFC 7231, 6.5.7
	StatusConflict                     = 409 // RFC 7231, 6.5.8
	StatusGone                         = 410 // RFC 7231, 6.5.9
	StatusLengthRequired               = 411 // RFC 7231, 6.5.10
	StatusPreconditionFailed           = 412 // RFC 7232, 4.2
	StatusRequestEntityTooLarge        = 413 // RFC 7231, 6.5.11
	StatusRequestURITooLong            = 414 // RFC 7231, 6.5.12
	StatusUnsupportedMediaType         = 415 // RFC 7231, 6.5.13
	StatusRequestedRangeNotSatisfiable = 416 // RFC 7233, 4.4
	StatusExpectationFailed            = 417 // RFC 7231, 6.5.14
	StatusTeapot                       = 418 // RFC 7168, 2.3.3
	StatusMisdirectedRequest           = 421 // RFC 7540, 9.1.2
	StatusUnprocessableEntity          = 422 // RFC 4918, 11.2
	StatusLocked                       = 423 // RFC 4918, 11.3
	StatusFailedDependency             = 424 // RFC 4918, 11.4
	StatusTooEarly                     = 425 // RFC 8470, 5.2.
	StatusUpgradeRequired              = 426 // RFC 7231, 6.5.15
	StatusPreconditionRequired         = 428 // RFC 6585, 3
	StatusTooManyRequests              = 429 // RFC 6585, 4
	StatusRequestHeaderFieldsTooLarge  = 431 // RFC 6585, 5
	StatusUnavailableForLegalReasons   = 451 // RFC 7725, 3

	StatusInternalServerError           = 500 // RFC 7231, 6.6.1
	StatusNotImplemented                = 501 // RFC 7231, 6.6.2
	StatusBadGateway                    = 502 // RFC 7231, 6.6.3
	StatusServiceUnavailable            = 503 // RFC 7231, 6.6.4
	StatusGatewayTimeout                = 504 // RFC 7231, 6.6.5
	StatusHTTPVersionNotSupported       = 505 // RFC 7231, 6.6.6
	StatusVariantAlsoNegotiates         = 506 // RFC 2295, 8.1
	StatusInsufficientStorage           = 507 // RFC 4918, 11.5
	StatusLoopDetected                  = 508 // RFC 5842, 7.2
	StatusNotExtended                   = 510 // RFC 2774, 7
	StatusNetworkAuthenticationRequired = 511 // RFC 6585, 6
)

var statusText = map[int]string{
	StatusContinue:           "Continue",
	StatusSwitchingProtocols: "Switching Protocols",
	StatusProcessing:         "Processing",
	StatusEarlyHints:         "Early Hints",

	StatusOK:                   "OK",
	StatusCreated:              "Created",
	StatusAccepted:             "Accepted",
	StatusNonAuthoritativeInfo: "Non-Authoritative Information",
	StatusNoContent:            "No Content",
	StatusResetContent:         "Reset Content",
	StatusPartialContent:       "Partial Content",
	StatusMultiStatus:          "Multi-Status",
	StatusAlreadyReported:      "Already Reported",
	StatusIMUsed:               "IM Used",

	StatusMultipleChoices:   "Multiple Choices",
	StatusMovedPermanently:  "Moved Permanently",
	StatusFound:             "Found",
	StatusSeeOther:          "See Other",
	StatusNotModified:       "Not Modified",
	StatusUseProxy:          "Use Proxy",
	StatusTemporaryRedirect: "Temporary Redirect",
	StatusPermanentRedirect: "Permanent Redirect",

	StatusBadRequest:                   "Bad Request",
	StatusUnauthorized:                 "Unauthorized",
	StatusPaymentRequired:              "Payment Required",
	StatusForbidden:                    "Forbidden",
	StatusNotFound:                     "Not Found",
	StatusMethodNotAllowed:             "Method Not Allowed",
	StatusNotAcceptable:                "Not Acceptable",
	StatusProxyAuthRequired:            "Proxy Authentication Required",
	StatusRequestTimeout:               "Request Timeout",
	StatusConflict:                     "Conflict",
	StatusGone:                         "Gone",
	StatusLengthRequired:               "Length Required",
	StatusPreconditionFailed:           "Precondition Failed",
	StatusRequestEntityTooLarge:        "Request Entity Too Large",
	StatusRequestURITooLong:            "Request URI Too Long",
	StatusUnsupportedMediaType:         "Unsupported Media Type",
	StatusRequestedRangeNotSatisfiable: "Requested Range Not Satisfiable",
	StatusExpectationFailed:            "Expectation Failed",
	StatusTeapot:                       "I'm a teapot",
	StatusMisdirectedRequest:           "Misdirected Request",
	StatusUnprocessableEntity:          "Unprocessable Entity",
	StatusLocked:                       "Locked",
	StatusFailedDependency:             "Failed Dependency",
	StatusTooEarly:                     "Too Early",
	StatusUpgradeRequired:              "Upgrade Required",
	StatusPreconditionRequired:         "Precondition Required",
	StatusTooManyRequests:              "Too Many Requests",
	StatusRequestHeaderFieldsTooLarge:  "Request Header Fields Too Large",
	StatusUnavailableForLegalReasons:   "Unavailable For Legal Reasons",

	StatusInternalServerError:           "Internal Server Error",
	StatusNotImplemented:                "Not Implemented",
	StatusBadGateway:                    "Bad Gateway",
	StatusServiceUnavailable:            "Service Unavailable",
	StatusGatewayTimeout:                "Gateway Timeout",
	StatusHTTPVersionNotSupported:       "HTTP Version Not Supported",
	StatusVariantAlsoNegotiates:         "Variant Also Negotiates",
	StatusInsufficientStorage:           "Insufficient Storage",
	StatusLoopDetected:                  "Loop Detected",
	StatusNotExtended:                   "Not Extended",
	StatusNetworkAuthenticationRequired: "Network Authentication Required",
}

// StatusText returns a text for the HTTP status code. It returns the empty
// string if the code is unknown.
func StatusText(code int) string {
	return statusText[code]
}

var types = map[string]string{
	".3dm":       "x-world/x-3dmf",
	".3dmf":      "x-world/x-3dmf",
	".7z":        "application/x-7z-compressed",
	".a":         "application/octet-stream",
	".aab":       "application/x-authorware-bin",
	".aam":       "application/x-authorware-map",
	".aas":       "application/x-authorware-seg",
	".abc":       "text/vndabc",
	".ace":       "application/x-ace-compressed",
	".acgi":      "text/html",
	".afl":       "video/animaflex",
	".ai":        "application/postscript",
	".aif":       "audio/aiff",
	".aifc":      "audio/aiff",
	".aiff":      "audio/aiff",
	".aim":       "application/x-aim",
	".aip":       "text/x-audiosoft-intra",
	".alz":       "application/x-alz-compressed",
	".ani":       "application/x-navi-animation",
	".aos":       "application/x-nokia-9000-communicator-add-on-software",
	".aps":       "application/mime",
	".apk":       "application/vnd.android.package-archive",
	".arc":       "application/x-arc-compressed",
	".arj":       "application/arj",
	".art":       "image/x-jg",
	".asf":       "video/x-ms-asf",
	".asm":       "text/x-asm",
	".asp":       "text/asp",
	".asx":       "application/x-mplayer2",
	".au":        "audio/basic",
	".avi":       "video/x-msvideo",
	".avs":       "video/avs-video",
	".bcpio":     "application/x-bcpio",
	".bin":       "application/mac-binary",
	".bmp":       "image/bmp",
	".boo":       "application/book",
	".book":      "application/book",
	".boz":       "application/x-bzip2",
	".bsh":       "application/x-bsh",
	".bz2":       "application/x-bzip2",
	".bz":        "application/x-bzip",
	".c++":       "text/plain",
	".c":         "text/x-c",
	".cab":       "application/vnd.ms-cab-compressed",
	".cat":       "application/vndms-pkiseccat",
	".cc":        "text/x-c",
	".ccad":      "application/clariscad",
	".cco":       "application/x-cocoa",
	".cdf":       "application/cdf",
	".cer":       "application/pkix-cert",
	".cha":       "application/x-chat",
	".chat":      "application/x-chat",
	".chrt":      "application/vnd.kde.kchart",
	".class":     "application/java",
	".com":       "text/plain",
	".conf":      "text/plain",
	".cpio":      "application/x-cpio",
	".cpp":       "text/x-c",
	".cpt":       "application/mac-compactpro",
	".crl":       "application/pkcs-crl",
	".crt":       "application/pkix-cert",
	".crx":       "application/x-chrome-extension",
	".csh":       "text/x-scriptcsh",
	".css":       "text/css",
	".csv":       "text/csv",
	".cxx":       "text/plain",
	".dar":       "application/x-dar",
	".dcr":       "application/x-director",
	".deb":       "application/x-debian-package",
	".deepv":     "application/x-deepv",
	".def":       "text/plain",
	".der":       "application/x-x509-ca-cert",
	".dif":       "video/x-dv",
	".dir":       "application/x-director",
	".divx":      "video/divx",
	".dl":        "video/dl",
	".dmg":       "application/x-apple-diskimage",
	".doc":       "application/msword",
	".dot":       "application/msword",
	".dp":        "application/commonground",
	".drw":       "application/drafting",
	".dump":      "application/octet-stream",
	".dv":        "video/x-dv",
	".dvi":       "application/x-dvi",
	".dwf":       "drawing/x-dwf=(old)",
	".dwg":       "application/acad",
	".dxf":       "application/dxf",
	".dxr":       "application/x-director",
	".el":        "text/x-scriptelisp",
	".elc":       "application/x-bytecodeelisp=(compiled=elisp)",
	".eml":       "message/rfc822",
	".env":       "application/x-envoy",
	".eps":       "application/postscript",
	".es":        "application/x-esrehber",
	".etx":       "text/x-setext",
	".evy":       "application/envoy",
	".exe":       "application/octet-stream",
	".f77":       "text/x-fortran",
	".f90":       "text/x-fortran",
	".f":         "text/x-fortran",
	".fdf":       "application/vndfdf",
	".fif":       "application/fractals",
	".fli":       "video/fli",
	".flo":       "image/florian",
	".flv":       "video/x-flv",
	".flx":       "text/vndfmiflexstor",
	".fmf":       "video/x-atomic3d-feature",
	".for":       "text/x-fortran",
	".fpx":       "image/vndfpx",
	".frl":       "application/freeloader",
	".funk":      "audio/make",
	".g3":        "image/g3fax",
	".g":         "text/plain",
	".gif":       "image/gif",
	".gl":        "video/gl",
	".gsd":       "audio/x-gsm",
	".gsm":       "audio/x-gsm",
	".gsp":       "application/x-gsp",
	".gss":       "application/x-gss",
	".gtar":      "application/x-gtar",
	".gz":        "application/x-compressed",
	".gzip":      "application/x-gzip",
	".h":         "text/x-h",
	".hdf":       "application/x-hdf",
	".help":      "application/x-helpfile",
	".hgl":       "application/vndhp-hpgl",
	".hh":        "text/x-h",
	".hlb":       "text/x-script",
	".hlp":       "application/hlp",
	".hpg":       "application/vndhp-hpgl",
	".hpgl":      "application/vndhp-hpgl",
	".hqx":       "application/binhex",
	".hta":       "application/hta",
	".htc":       "text/x-component",
	".htm":       "text/html",
	".html":      "text/html",
	".htmls":     "text/html",
	".htt":       "text/webviewhtml",
	".htx":       "text/html",
	".ice":       "x-conference/x-cooltalk",
	".ico":       "image/x-icon",
	".ics":       "text/calendar",
	".icz":       "text/calendar",
	".idc":       "text/plain",
	".ief":       "image/ief",
	".iefs":      "image/ief",
	".iges":      "application/iges",
	".igs":       "application/iges",
	".ima":       "application/x-ima",
	".imap":      "application/x-httpd-imap",
	".inf":       "application/inf",
	".ins":       "application/x-internett-signup",
	".ip":        "application/x-ip2",
	".isu":       "video/x-isvideo",
	".it":        "audio/it",
	".iv":        "application/x-inventor",
	".ivr":       "i-world/i-vrml",
	".ivy":       "application/x-livescreen",
	".jam":       "audio/x-jam",
	".jav":       "text/x-java-source",
	".java":      "text/x-java-source",
	".jcm":       "application/x-java-commerce",
	".jfif-tbnl": "image/jpeg",
	".jfif":      "image/jpeg",
	".jnlp":      "application/x-java-jnlp-file",
	".jpe":       "image/jpeg",
	".jpeg":      "image/jpeg",
	".jpg":       "image/jpeg",
	".jps":       "image/x-jps",
	".js":        "application/javascript",
	".json":      "application/json",
	".jut":       "image/jutvision",
	".kar":       "audio/midi",
	".karbon":    "application/vnd.kde.karbon",
	".kfo":       "application/vnd.kde.kformula",
	".flw":       "application/vnd.kde.kivio",
	".kml":       "application/vnd.google-earth.kml+xml",
	".kmz":       "application/vnd.google-earth.kmz",
	".kon":       "application/vnd.kde.kontour",
	".kpr":       "application/vnd.kde.kpresenter",
	".kpt":       "application/vnd.kde.kpresenter",
	".ksp":       "application/vnd.kde.kspread",
	".kwd":       "application/vnd.kde.kword",
	".kwt":       "application/vnd.kde.kword",
	".ksh":       "text/x-scriptksh",
	".la":        "audio/nspaudio",
	".lam":       "audio/x-liveaudio",
	".latex":     "application/x-latex",
	".lha":       "application/lha",
	".lhx":       "application/octet-stream",
	".list":      "text/plain",
	".lma":       "audio/nspaudio",
	".log":       "text/plain",
	".lsp":       "text/x-scriptlisp",
	".lst":       "text/plain",
	".lsx":       "text/x-la-asf",
	".ltx":       "application/x-latex",
	".lzh":       "application/octet-stream",
	".lzx":       "application/lzx",
	".m1v":       "video/mpeg",
	".m2a":       "audio/mpeg",
	".m2v":       "video/mpeg",
	".m3u":       "audio/x-mpegurl",
	".m":         "text/x-m",
	".man":       "application/x-troff-man",
	".manifest":  "text/cache-manifest",
	".map":       "application/x-navimap",
	".mar":       "text/plain",
	".mbd":       "application/mbedlet",
	".mc$":       "application/x-magic-cap-package-10",
	".mcd":       "application/mcad",
	".mcf":       "text/mcf",
	".mcp":       "application/netmc",
	".me":        "application/x-troff-me",
	".mht":       "message/rfc822",
	".mhtml":     "message/rfc822",
	".mid":       "application/x-midi",
	".midi":      "application/x-midi",
	".mif":       "application/x-frame",
	".mime":      "message/rfc822",
	".mjf":       "audio/x-vndaudioexplosionmjuicemediafile",
	".mjpg":      "video/x-motion-jpeg",
	".mm":        "application/base64",
	".mme":       "application/base64",
	".mod":       "audio/mod",
	".moov":      "video/quicktime",
	".mov":       "video/quicktime",
	".movie":     "video/x-sgi-movie",
	".mp2":       "audio/mpeg",
	".mp3":       "audio/mpeg",
	".mp4":       "video/mp4",
	".mpa":       "audio/mpeg",
	".mpc":       "application/x-project",
	".mpe":       "video/mpeg",
	".mpeg":      "video/mpeg",
	".mpg":       "video/mpeg",
	".mpga":      "audio/mpeg",
	".mpp":       "application/vndms-project",
	".mpt":       "application/x-project",
	".mpv":       "application/x-project",
	".mpx":       "application/x-project",
	".mrc":       "application/marc",
	".ms":        "application/x-troff-ms",
	".mv":        "video/x-sgi-movie",
	".my":        "audio/make",
	".mzz":       "application/x-vndaudioexplosionmzz",
	".nap":       "image/naplps",
	".naplps":    "image/naplps",
	".nc":        "application/x-netcdf",
	".ncm":       "application/vndnokiaconfiguration-message",
	".nif":       "image/x-niff",
	".niff":      "image/x-niff",
	".nix":       "application/x-mix-transfer",
	".nsc":       "application/x-conference",
	".nvd":       "application/x-navidoc",
	".o":         "application/octet-stream",
	".oda":       "application/oda",
	".odb":       "application/vnd.oasis.opendocument.database",
	".odc":       "application/vnd.oasis.opendocument.chart",
	".odf":       "application/vnd.oasis.opendocument.formula",
	".odg":       "application/vnd.oasis.opendocument.graphics",
	".odi":       "application/vnd.oasis.opendocument.image",
	".odm":       "application/vnd.oasis.opendocument.text-master",
	".odp":       "application/vnd.oasis.opendocument.presentation",
	".ods":       "application/vnd.oasis.opendocument.spreadsheet",
	".odt":       "application/vnd.oasis.opendocument.text",
	".oga":       "audio/ogg",
	".ogg":       "audio/ogg",
	".ogv":       "video/ogg",
	".omc":       "application/x-omc",
	".omcd":      "application/x-omcdatamaker",
	".omcr":      "application/x-omcregerator",
	".otc":       "application/vnd.oasis.opendocument.chart-template",
	".otf":       "application/vnd.oasis.opendocument.formula-template",
	".otg":       "application/vnd.oasis.opendocument.graphics-template",
	".oth":       "application/vnd.oasis.opendocument.text-web",
	".oti":       "application/vnd.oasis.opendocument.image-template",
	".otm":       "application/vnd.oasis.opendocument.text-master",
	".otp":       "application/vnd.oasis.opendocument.presentation-template",
	".ots":       "application/vnd.oasis.opendocument.spreadsheet-template",
	".ott":       "application/vnd.oasis.opendocument.text-template",
	".p10":       "application/pkcs10",
	".p12":       "application/pkcs-12",
	".p7a":       "application/x-pkcs7-signature",
	".p7c":       "application/pkcs7-mime",
	".p7m":       "application/pkcs7-mime",
	".p7r":       "application/x-pkcs7-certreqresp",
	".p7s":       "application/pkcs7-signature",
	".p":         "text/x-pascal",
	".part":      "application/pro_eng",
	".pas":       "text/pascal",
	".pbm":       "image/x-portable-bitmap",
	".pcl":       "application/vndhp-pcl",
	".pct":       "image/x-pict",
	".pcx":       "image/x-pcx",
	".pdb":       "chemical/x-pdb",
	".pdf":       "application/pdf",
	".pfunk":     "audio/make",
	".pgm":       "image/x-portable-graymap",
	".pic":       "image/pict",
	".pict":      "image/pict",
	".pkg":       "application/x-newton-compatible-pkg",
	".pko":       "application/vndms-pkipko",
	".pl":        "text/x-scriptperl",
	".plx":       "application/x-pixclscript",
	".pm4":       "application/x-pagemaker",
	".pm5":       "application/x-pagemaker",
	".pm":        "text/x-scriptperl-module",
	".png":       "image/png",
	".pnm":       "application/x-portable-anymap",
	".pot":       "application/mspowerpoint",
	".pov":       "model/x-pov",
	".ppa":       "application/vndms-powerpoint",
	".ppm":       "image/x-portable-pixmap",
	".pps":       "application/mspowerpoint",
	".ppt":       "application/mspowerpoint",
	".ppz":       "application/mspowerpoint",
	".pre":       "application/x-freelance",
	".prt":       "application/pro_eng",
	".ps":        "application/postscript",
	".psd":       "application/octet-stream",
	".pvu":       "paleovu/x-pv",
	".pwz":       "application/vndms-powerpoint",
	".py":        "text/x-scriptphyton",
	".pyc":       "application/x-bytecodepython",
	".qcp":       "audio/vndqcelp",
	".qd3":       "x-world/x-3dmf",
	".qd3d":      "x-world/x-3dmf",
	".qif":       "image/x-quicktime",
	".qt":        "video/quicktime",
	".qtc":       "video/x-qtc",
	".qti":       "image/x-quicktime",
	".qtif":      "image/x-quicktime",
	".ra":        "audio/x-pn-realaudio",
	".ram":       "audio/x-pn-realaudio",
	".rar":       "application/x-rar-compressed",
	".ras":       "application/x-cmu-raster",
	".rast":      "image/cmu-raster",
	".rexx":      "text/x-scriptrexx",
	".rf":        "image/vndrn-realflash",
	".rgb":       "image/x-rgb",
	".rm":        "application/vndrn-realmedia",
	".rmi":       "audio/mid",
	".rmm":       "audio/x-pn-realaudio",
	".rmp":       "audio/x-pn-realaudio",
	".rng":       "application/ringing-tones",
	".rnx":       "application/vndrn-realplayer",
	".roff":      "application/x-troff",
	".rp":        "image/vndrn-realpix",
	".rpm":       "audio/x-pn-realaudio-plugin",
	".rt":        "text/vndrn-realtext",
	".rtf":       "text/richtext",
	".rtx":       "text/richtext",
	".rv":        "video/vndrn-realvideo",
	".s":         "text/x-asm",
	".s3m":       "audio/s3m",
	".s7z":       "application/x-7z-compressed",
	".saveme":    "application/octet-stream",
	".sbk":       "application/x-tbook",
	".scm":       "text/x-scriptscheme",
	".sdml":      "text/plain",
	".sdp":       "application/sdp",
	".sdr":       "application/sounder",
	".sea":       "application/sea",
	".set":       "application/set",
	".sgm":       "text/x-sgml",
	".sgml":      "text/x-sgml",
	".sh":        "text/x-scriptsh",
	".shar":      "application/x-bsh",
	".shtml":     "text/x-server-parsed-html",
	".sid":       "audio/x-psid",
	".skd":       "application/x-koan",
	".skm":       "application/x-koan",
	".skp":       "application/x-koan",
	".skt":       "application/x-koan",
	".sit":       "application/x-stuffit",
	".sitx":      "application/x-stuffitx",
	".sl":        "application/x-seelogo",
	".smi":       "application/smil",
	".smil":      "application/smil",
	".snd":       "audio/basic",
	".sol":       "application/solids",
	".spc":       "text/x-speech",
	".spl":       "application/futuresplash",
	".spr":       "application/x-sprite",
	".sprite":    "application/x-sprite",
	".spx":       "audio/ogg",
	".src":       "application/x-wais-source",
	".ssi":       "text/x-server-parsed-html",
	".ssm":       "application/streamingmedia",
	".sst":       "application/vndms-pkicertstore",
	".step":      "application/step",
	".stl":       "application/sla",
	".stp":       "application/step",
	".sv4cpio":   "application/x-sv4cpio",
	".sv4crc":    "application/x-sv4crc",
	".svf":       "image/vnddwg",
	".svg":       "image/svg+xml",
	".svr":       "application/x-world",
	".swf":       "application/x-shockwave-flash",
	".t":         "application/x-troff",
	".talk":      "text/x-speech",
	".tar":       "application/x-tar",
	".tbk":       "application/toolbook",
	".tcl":       "text/x-scripttcl",
	".tcsh":      "text/x-scripttcsh",
	".tex":       "application/x-tex",
	".texi":      "application/x-texinfo",
	".texinfo":   "application/x-texinfo",
	".text":      "text/plain",
	".tgz":       "application/gnutar",
	".tif":       "image/tiff",
	".tiff":      "image/tiff",
	".tr":        "application/x-troff",
	".tsi":       "audio/tsp-audio",
	".tsp":       "application/dsptype",
	".tsv":       "text/tab-separated-values",
	".turbot":    "image/florian",
	".txt":       "text/plain",
	".uil":       "text/x-uil",
	".uni":       "text/uri-list",
	".unis":      "text/uri-list",
	".unv":       "application/i-deas",
	".uri":       "text/uri-list",
	".uris":      "text/uri-list",
	".ustar":     "application/x-ustar",
	".uu":        "text/x-uuencode",
	".uue":       "text/x-uuencode",
	".vcd":       "application/x-cdlink",
	".vcf":       "text/x-vcard",
	".vcard":     "text/x-vcard",
	".vcs":       "text/x-vcalendar",
	".vda":       "application/vda",
	".vdo":       "video/vdo",
	".vew":       "application/groupwise",
	".viv":       "video/vivo",
	".vivo":      "video/vivo",
	".vmd":       "application/vocaltec-media-desc",
	".vmf":       "application/vocaltec-media-file",
	".voc":       "audio/voc",
	".vos":       "video/vosaic",
	".vox":       "audio/voxware",
	".vqe":       "audio/x-twinvq-plugin",
	".vqf":       "audio/x-twinvq",
	".vql":       "audio/x-twinvq-plugin",
	".vrml":      "application/x-vrml",
	".vrt":       "x-world/x-vrt",
	".vsd":       "application/x-visio",
	".vst":       "application/x-visio",
	".vsw":       "application/x-visio",
	".w60":       "application/wordperfect60",
	".w61":       "application/wordperfect61",
	".w6w":       "application/msword",
	".wav":       "audio/wav",
	".wb1":       "application/x-qpro",
	".wbmp":      "image/vnd.wap.wbmp",
	".web":       "application/vndxara",
	".wiz":       "application/msword",
	".wk1":       "application/x-123",
	".wmf":       "windows/metafile",
	".wml":       "text/vnd.wap.wml",
	".wmlc":      "application/vnd.wap.wmlc",
	".wmls":      "text/vnd.wap.wmlscript",
	".wmlsc":     "application/vnd.wap.wmlscriptc",
	".word":      "application/msword",
	".wp5":       "application/wordperfect",
	".wp6":       "application/wordperfect",
	".wp":        "application/wordperfect",
	".wpd":       "application/wordperfect",
	".wq1":       "application/x-lotus",
	".wri":       "application/mswrite",
	".wrl":       "application/x-world",
	".wrz":       "model/vrml",
	".wsc":       "text/scriplet",
	".wsrc":      "application/x-wais-source",
	".wtk":       "application/x-wintalk",
	".x-png":     "image/png",
	".xbm":       "image/x-xbitmap",
	".xdr":       "video/x-amt-demorun",
	".xgz":       "xgl/drawing",
	".xif":       "image/vndxiff",
	".xl":        "application/excel",
	".xla":       "application/excel",
	".xlb":       "application/excel",
	".xlc":       "application/excel",
	".xld":       "application/excel",
	".xlk":       "application/excel",
	".xll":       "application/excel",
	".xlm":       "application/excel",
	".xls":       "application/excel",
	".xlt":       "application/excel",
	".xlv":       "application/excel",
	".xlw":       "application/excel",
	".xm":        "audio/xm",
	".xml":       "text/xml",
	".xmz":       "xgl/movie",
	".xpix":      "application/x-vndls-xpix",
	".xpm":       "image/x-xpixmap",
	".xsr":       "video/x-amt-showrun",
	".xwd":       "image/x-xwd",
	".xyz":       "chemical/x-pdb",
	".z":         "application/x-compress",
	".zip":       "application/zip",
	".zoo":       "application/octet-stream",
	".zsh":       "text/x-scriptzsh",
	".docx":      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	".docm":      "application/vnd.ms-word.document.macroEnabled.12",
	".dotx":      "application/vnd.openxmlformats-officedocument.wordprocessingml.template",
	".dotm":      "application/vnd.ms-word.template.macroEnabled.12",
	".xlsx":      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	".xlsm":      "application/vnd.ms-excel.sheet.macroEnabled.12",
	".xltx":      "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
	".xltm":      "application/vnd.ms-excel.template.macroEnabled.12",
	".xlsb":      "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
	".xlam":      "application/vnd.ms-excel.addin.macroEnabled.12",
	".pptx":      "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	".pptm":      "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
	".ppsx":      "application/vnd.openxmlformats-officedocument.presentationml.slideshow",
	".ppsm":      "application/vnd.ms-powerpoint.slideshow.macroEnabled.12",
	".potx":      "application/vnd.openxmlformats-officedocument.presentationml.template",
	".potm":      "application/vnd.ms-powerpoint.template.macroEnabled.12",
	".ppam":      "application/vnd.ms-powerpoint.addin.macroEnabled.12",
	".sldx":      "application/vnd.openxmlformats-officedocument.presentationml.slide",
	".sldm":      "application/vnd.ms-powerpoint.slide.macroEnabled.12",
	".thmx":      "application/vnd.ms-officetheme",
	".onetoc":    "application/onenote",
	".onetoc2":   "application/onenote",
	".onetmp":    "application/onenote",
	".onepkg":    "application/onenote",
	".xpi":       "application/x-xpinstall",
	".wasm":      "application/wasm",
}

func init() {
	for ext, typ := range types {
		// skip errors
		mime.AddExtensionType(ext, typ)
	}
}

// TypeByExtension returns the MIME type associated with the file extension ext.
// The extension ext should begin with a leading dot, as in ".html".
// When ext has no associated type, typeByExtension returns "".
//
// Extensions are looked up first case-sensitively, then case-insensitively.
//
// The built-in table is small but on unix it is augmented by the local
// system's mime.types file(s) if available under one or more of these
// names:
//
//   /etc/mime.types
//   /etc/apache2/mime.types
//   /etc/apache/mime.types
//
// On Windows, MIME types are extracted from the registry.
//
// Text types have the charset parameter set to "utf-8" by default.
func TypeByExtension(ext string) (typ string) {
	if len(ext) < 2 {
		return
	}

	if ext[0] != '.' { // try to take it by filename
		typ = TypeByFilename(ext)
		if typ == "" {
			ext = "." + ext // if error or something wrong then prepend the dot
		}
	}

	if typ == "" {
		typ = mime.TypeByExtension(ext)
	}

	// mime.TypeByExtension returns as text/plain; | charset=utf-8 the static .js (not always)
	if ext == ".js" && (typ == "text/plain" || typ == "text/plain; charset=utf-8") {

		if ext == ".js" {
			typ = "application/javascript"
		}
	}
	return typ
}

// TypeByFilename same as TypeByExtension
// but receives a filename path instead.
func TypeByFilename(fullFilename string) string {
	ext := filepath.Ext(fullFilename)
	return TypeByExtension(ext)
}
