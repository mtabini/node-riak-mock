function jsonFormatter(req, res, data) {
    if (data instanceof Error) {
        res.setHeader('Content-type', 'text/plain');
        data = data.message;
    } else {
        data = JSON.stringify(data);
    }
    
    return data;
}

module.exports = jsonFormatter;