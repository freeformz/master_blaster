require 'pg'

module PostgresConnection

  def setup_pg_connection(url, app_name)
    @uri = URI.parse(url)
    set_connection_app_name(app_name)
  end

  def set_connection_app_name(app_name)
    log(class: self.class, fn: :set_connection_app_name, to: app_name) do
      conn.exec("set application_name = #{app_name}")
    end
  end

  private

  def conn
    @conn ||= begin
                log(class: self.class, fn: :conn, new: true) do
                  ::PGconn.new(
                    host:     @uri.host,
                    user:     @uri.user,
                    dbname:   @uri.path[1..-1],
                    password: @uri.password,
                    port:     @uri.port,
                    #sslmode:  'require',
                    connect_timeout: 20
                  )
                end
              end
  end

  def unlisten(event)
    log(class: self.class, fn: :unlisten, event: event) do
      conn.exec("UNLISTEN #{event}")
    end
  end

  def listen_for(event)
    log(class: self.class, fn: :listen_for, event: event) do
      conn.exec("LISTEN #{event}")
    end
  end

  def notify(event, payload=nil)
    log(class: self.class, fn: :notify, event: event, payload: payload) do
      msg = "NOTIFY #{event}" 
      msg += ", '#{PGconn.escape_string(payload.to_s)}'" if payload
      conn.exec(msg)
    end
  end
end
