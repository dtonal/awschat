var ChatApp = window.ChatApp || {};

(function scopeWrapper($) {

    var currentUsername = 'Student';
    var apiEndpoint = ChatApp.apiEndpoint;
    var baseUrl = 'https://gbdfidk6cb.execute-api.eu-central-1.amazonaws.com/dev'
    ChatApp.populateChats = function () {
        $.get(baseUrl + '/conversations').done(function (data) {
            data.forEach(function (convo) {
                var otherUsers = [];
                convo.participants.forEach(function (user) {
                    if (user !== currentUsername) {
                        otherUsers.push(user);
                    }
                });

                $('TBODY').append('<tr><td><a href="chat.html#' + convo.id + '">' + otherUsers.join(', ') + '</a></td></tr>');
            });
            $('TBODY').append('<tr><td></td></tr>');
        });
    };

    ChatApp.loadChat = function () {
        $.get(baseUrl + '/conversations/' + location.hash.substring(1)).done(function (result) {
            result.messages.forEach(function (message) {
                var panel = $('<div class="panel">');
                if (message.sender === currentUsername) {
                    panel.addClass('panel-default');
                } else {
                    panel.addClass('panel-info');
                    panel.append('<div class="panel-heading">' + message.sender + '</div>');
                }
                var body = $('<div class="panel-body">').text(message.message);
                panel.append(body);
                panel.append('<div class="panel-footer messageTime" data-time="' + message.time + '">' + moment(parseInt(message.time)).fromNow() + '</div>');

                var row = $('<div class="row">');
                var buffer = $('<div class="col-xs-4">');
                var holder = $('<div class="col-xs-8">');
                holder.append(panel);

                if (message.sender === currentUsername) {
                    row.append(buffer);
                    row.append(holder);
                } else {
                    row.append(holder);
                    row.append(buffer);
                }

                $('#chat').append(row);
            });
            window.scrollTo(0, document.body.scrollHeight);
        });
    };
}(jQuery));