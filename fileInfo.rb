#!/usr/bin/env ruby
require 'redis'
require 'mongo'
require 'find'

@conn=Mongo::ReplSetConnection.new(['mgd-dev01.aws:27017', 
                                    'mgd-dev02.aws:27017', 
                                    'mgd-dev03.aws:27017'], 
  :read => :secondary, :refresh_mode => :sync, :refresh_interval => 60)

@db=@conn['dbame']
@coll=@db['collection']
@log=@db['loggingcollection']

Find.find("/path/to/stored/files") do |file|
  if not File.directory?(file)

    accountid, application = file.split("/")[7,2]
    begin
      f=File::Stat.new(file)
      mode, uid, gid, size, blksize, blocks, atime, mtime, ctime = f.mode, f.uid, f.gid, f.size, f.blksize, f.blocks, f.atime, f.mtime, f.ctime

      doc={"filename" => file, "tags" => {"accountid" => accountid, "application" => application}, "mode" => mode, "uid" => uid, "gid" => gid, "size" => size, "blksize" => blksize, "blocks" => blocks, "atime" => atime, "mtime" => mtime, "ctime" => ctime}
      id=@coll.insert(doc)
    rescue => e
      #p "File #{file} raised an exception"
      logentry={"filename" => file, "exception" => 'true', "entry" => e.message}
      id=@log.insert(logentry)
    ensure
      #puts "Inserted items with id of #{id}"
      logentry={"filename" => file, "exception" => 'false', "entry" => "Processed items for file."}
      id = @log.insert(logentry)
      doc = nil      
      logentry = nil
    end
  end
end
