require 'mongo'

# conn = Mongo::Connection.new("mgd-dev01.aws.")
# db=conn['fileinfo']
# coll=db['fooresults']

conn = Mongo::Connection.new()
db=conn['test']
coll=db['jalons']

# FNAME = "useraccounts.ibp-prod.csv"
FNAME="files.txt"

f=File.open(FNAME, "r")

#ids=[]
#f.each_line do |acctid| 
#  ids << acctid.to_s.chomp
#end

sum=0.0
resultArray=[]
count=0
f.each do |acctid| 
  #puts acctid.to_s
  #print "Testing #{acctid.to_s.chomp} :: "
  coll.find("_id" => acctid.to_s.chomp).each do |record| 
     resultArray[count] = Hash.new
     resultArray[count][:acctid] = record["value"]["size"]
     count+=1
     print "Processed: #{acctid.to_s.chomp} : #{record["value"]["size"]}"
     sum+=record["value"]["size"].to_i
   end
   puts "\n"
end

puts "Sum: #{sum} Bytes"
sum = sum/1024
puts "Kilobytes: #{sum}"
sum = sum/1024
puts "Megabytes: #{sum}"
sum = sum/1024
puts "Gigabytes: #{sum}"