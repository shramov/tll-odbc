#ifndef _CHANNEL_HEARTBEAT_H
#define _CHANNEL_HEARTBEAT_H

#include <tll/channel/prefix.h>
#include <tll/util/time.h>

#include <fmt/chrono.h>

class HeartBeat : public tll::channel::Prefix<HeartBeat>
{
	using Base = tll::channel::Prefix<HeartBeat>;
	tll::time_point _last = {};
	tll::duration _timeout = {};
	std::string _message_name;
	tll_msg_t _msg = {};
	std::vector<char> _buf;
	std::unique_ptr<tll::Channel> _timer;

 public:
	static constexpr std::string_view channel_protocol() { return "db-heartbeat+"; }

	int _init(const tll::Channel::Url &cfg, tll::Channel *master)
	{
		using namespace std::chrono_literals;

		if (auto r = Base::_init(cfg, master); r)
			return r;

		auto reader = channel_props_reader(cfg);
		_timeout = reader.getT<tll::duration>("timeout", 1s);
		_message_name = reader.getT<std::string>("message");
		if (!reader)
			return _log.fail(EINVAL, "Invalid url: {}", reader.error());

		auto curl = child_url_parse("timer://;dump=yes", "timer");
		if (!curl)
			return _log.fail(EINVAL, "Failed to parse timer url: {}", curl.error());
		curl->set("clock", "monotonic");
		curl->set("interval", fmt::format("{}", _timeout / 2));
		_timer = context().channel(*curl);
		if (!_timer)
			return _log.fail(EINVAL, "Failed to create timer channel");

		_timer->callback_add<HeartBeat, &HeartBeat::_on_timer>(this, TLL_MESSAGE_MASK_DATA);
		_child_add(_timer.get(), "timer");
		return 0;
	}

	int _on_active()
	{
		auto s = scheme(TLL_MESSAGE_DATA);
		if (!s)
			return _log.fail(EINVAL, "Channel requires scheme");
		auto message = s->lookup(_message_name);
		if (!message)
			return _log.fail(EINVAL, "Message {} not found in scheme", _message_name);
		_msg.msgid = message->msgid;
		_buf.resize(message->size);
		_msg.data = _buf.data();
		_msg.size = _buf.size();

		_last = tll::time::now();
		if (auto r = _timer->open(); r)
			return _log.fail(EINVAL, "Failed to open timer");
		return Base::_on_active();
	}

	int _on_closing()
	{
		_timer->close();
		_msg = {};
		return Base::_on_closing();
	}

	int _on_timer(const tll::Channel *, const tll_msg_t *)
	{
		auto now = tll::time::now();
		if (_last + _timeout > now)
			return 0;
		_last = now;
		if (auto r = _child->post(&_msg); r)
			return state_fail(r, "Heartbeat post failed");
		return 0;
	}


	int _post(const tll_msg_t * msg, int flags)
	{
		_last = tll::time::now();
		return Base::_post(msg, flags);
	}

	int _on_data(const tll_msg_t * msg)
	{
		_last = tll::time::now();
		return Base::_on_data(msg);
	}
};

#endif//_CHANNEL_HEARTBEAT_H
