require 'mongo'

conn = Mongo::Connection.new("mgd-dev01.aws.ifbyphone.com")
db=conn['fileinfo']
coll=db['fooresults']

#conn = Mongo::Connection.new()
#db=conn['test']
#coll=db['jalons']

FNAME = "useraccounts.ibp-prod.csv"
#FNAME="files.txt"

f=File.open(FNAME, "r")

ids=[]
f.each_line do |acctid| 
  ids << acctid.to_s.chomp
end

sum=0.0
resultArray=[]
count=0
ignoredCount=0

coll.find.each do |record| 
   if ids.include? record["_id"] then
     resultArray[count] = Hash.new
     resultArray[count][:acctid] = record["value"]["size"]
     count+=1
     print "Processed: #{record["_id"]} :: #{record["value"]["size"]}\n" if $DEBUG
     sum+=record["value"]["size"].to_i
   else
     puts "#{record["_id"]} not found" if $DEBUG
     ignoredCount+=1
   end
end
 puts "\n" if $DEBUG

puts "Sum: #{sum} Bytes"
sum = sum/1024
puts "Kilobytes: #{sum}"
sum = sum/1024
puts "Megabytes: #{sum}"
sum = sum/1024
puts "Gigabytes: #{sum}"
puts "Found #{count} cancelled accounts with data"
puts "Found #{ignoredCount} cancelled accounts without data."